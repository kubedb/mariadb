/*
Copyright AppsCode Inc. and Contributors

Licensed under the AppsCode Community License 1.0.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://github.com/appscode/licenses/raw/1.0.0/AppsCode-Community-1.0.0.md

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"
	"strings"

	meta_util "kmodules.xyz/client-go/meta"

	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"
	"kubedb.dev/apimachinery/pkg/eventer"

	"gomodules.xyz/pointer"
	"gomodules.xyz/x/log"
	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kutil "kmodules.xyz/client-go"
	app_util "kmodules.xyz/client-go/apps/v1"
	core_util "kmodules.xyz/client-go/core/v1"
	meta_util "kmodules.xyz/client-go/meta"
	mona "kmodules.xyz/monitoring-agent-api/api/v1"
	ofst "kmodules.xyz/offshoot-api/api/v1"
)

type workloadOptions struct {
	// App level options
	stsName   string
	labels    map[string]string
	selectors map[string]string

	// db container options
	conatainerName string
	image          string
	cmd            []string // cmd of `mariadb` container
	args           []string // args of `mariadb` container
	ports          []core.ContainerPort
	envList        []core.EnvVar // envList of `mariadb` container
	volumeMount    []core.VolumeMount
	configSecret   *core.LocalObjectReference

	// monitor container
	monitorContainer *core.Container

	// pod Template level options
	replicas       *int32
	gvrSvcName     string
	podTemplate    *ofst.PodTemplateSpec
	pvcSpec        *core.PersistentVolumeClaimSpec
	initContainers []core.Container
	volume         []core.Volume // volumes to mount on stsPodTemplate
}

func (c *Controller) ensureStatefulSet(db *api.MariaDB) (kutil.VerbType, error) {
	dbVersion, err := c.DBClient.CatalogV1alpha1().MariaDBVersions().Get(context.TODO(), string(db.Spec.Version), metav1.GetOptions{})
	if err != nil {
		return kutil.VerbUnchanged, err
	}

	// container for removing lost and found
	initContainers := []core.Container{
		{
			Name:            "remove-lost-found",
			Image:           dbVersion.Spec.InitContainer.Image,
			ImagePullPolicy: core.PullIfNotPresent,
			Command: []string{
				"rm",
				"-rf",
				"/var/lib/mysql/lost+found",
			},
			VolumeMounts: []core.VolumeMount{
				{
					Name:      "data",
					MountPath: api.MariaDBDataMountPath,
				},
			},
			Resources: db.Spec.PodTemplate.Spec.Resources,
		},
	}

	var ports = []core.ContainerPort{
		{
			Name:          api.MySQLDatabasePortName,
			ContainerPort: api.MySQLDatabasePort,
			Protocol:      core.ProtocolTCP,
		},
	}

	var cmds, args, tempArgs []string
	// Adding peer-finders and user provided arguments
	userProvidedArgs := db.Spec.PodTemplate.Spec.Args
	if db.IsCluster() {
		cmds = []string{
			"peer-finder",
		}
		args = []string{
			fmt.Sprintf("-service=%s", db.GoverningServiceName()),
			"-on-start",
		}
		tempArgs = append(tempArgs, "/on-start.sh")
	}

	tempArgs = append(tempArgs, userProvidedArgs...)

	// Adding arguments for TLS setup
	if db.Spec.TLS != nil {
		tlsArgs := []string{
			"--ssl-capath=/etc/mysql/certs/server",
			"--ssl-ca=/etc/mysql/certs/server/ca.crt",
			"--ssl-cert=/etc/mysql/certs/server/tls.crt",
			"--ssl-key=/etc/mysql/certs/server/tls.key",
		}
		if db.IsCluster() {
			tlsArgs = append(tlsArgs, "--wsrep-provider-options='socket.ssl_key=/etc/mysql/certs/server/tls.key;socket.ssl_cert=/etc/mysql/certs/server/tls.crt;socket.ssl_ca=/etc/mysql/certs/server/ca.crt'")
		}
		if db.Spec.RequireSSL {
			tlsArgs = append(tlsArgs, "--require-secure-transport=ON")
		}
		tempArgs = append(tempArgs, tlsArgs...)
	}
	if tempArgs != nil{
		if  db.IsCluster() {
			args = append(args, strings.Join(tempArgs, " "))
		} else {
			args = append(args, tempArgs...)
		}
	}


	var volumes []core.Volume
	var volumeMounts []core.VolumeMount

	if !db.IsCluster() && db.Spec.Init != nil && db.Spec.Init.Script != nil {
		volumes = append(volumes, core.Volume{
			Name:         "initial-script",
			VolumeSource: db.Spec.Init.Script.VolumeSource,
		})
		volumeMounts = append(volumeMounts, core.VolumeMount{
			Name:      "initial-script",
			MountPath: api.MariaDBInitDBMountPath,
		})
	}
	db.Spec.PodTemplate.Spec.ServiceAccountName = db.OffshootName()

	envList := []core.EnvVar{}
	if db.IsCluster() {
		envList = append(envList, core.EnvVar{
			Name:  "CLUSTER_NAME",
			Value: db.OffshootName(),
		})
	}
	var monitorContainer core.Container
	if db.Spec.Monitor != nil && db.Spec.Monitor.Agent.Vendor() == mona.VendorPrometheus {
		var commands []string
		// pass config.my-cnf flag into exporter to configure TLS
		if db.Spec.TLS != nil {
			// ref: https://github.com/prometheus/mysqld_exporter#general-flags
			// https://github.com/prometheus/mysqld_exporter#customizing-configuration-for-a-ssl-connection
			cmd := strings.Join(append([]string{
				"/bin/mysqld_exporter",
				fmt.Sprintf("--web.listen-address=:%d", db.Spec.Monitor.Prometheus.Exporter.Port),
				fmt.Sprintf("--web.telemetry-path=%v", db.StatsService().Path()),
				"--config.my-cnf=/etc/mysql/config/exporter/exporter.cnf",
			}, db.Spec.Monitor.Prometheus.Exporter.Args...), " ")
			commands = []string{cmd}
		} else {
			// DATA_SOURCE_NAME=user:password@tcp(localhost:5555)/dbname
			// ref: https://github.com/prometheus/mysqld_exporter#setting-the-mysql-servers-data-source-name
			cmd := strings.Join(append([]string{
				"/bin/mysqld_exporter",
				fmt.Sprintf("--web.listen-address=:%d", db.Spec.Monitor.Prometheus.Exporter.Port),
				fmt.Sprintf("--web.telemetry-path=%v", db.StatsService().Path()),
			}, db.Spec.Monitor.Prometheus.Exporter.Args...), " ")
			commands = []string{
				`export DATA_SOURCE_NAME="${MYSQL_ROOT_USERNAME:-}:${MYSQL_ROOT_PASSWORD:-}@(127.0.0.1:3306)/"`,
				cmd,
			}
		}
		script := strings.Join(commands, ";")
		monitorContainer = core.Container{
			Name: api.ContainerExporterName,
			Command: []string{
				"/bin/sh",
			},
			Args: []string{
				"-c",
				script,
			},
			Image: dbVersion.Spec.Exporter.Image,
			Ports: []core.ContainerPort{
				{
					Name:          mona.PrometheusExporterPortName,
					Protocol:      core.ProtocolTCP,
					ContainerPort: db.Spec.Monitor.Prometheus.Exporter.Port,
				},
			},
			Env:             db.Spec.Monitor.Prometheus.Exporter.Env,
			Resources:       db.Spec.Monitor.Prometheus.Exporter.Resources,
			SecurityContext: db.Spec.Monitor.Prometheus.Exporter.SecurityContext,
		}
	}

	opts := workloadOptions{
		stsName:          db.OffshootName(),
		labels:           db.OffshootLabels(),
		selectors:        db.OffshootSelectors(),
		conatainerName:   api.ResourceSingularMariaDB,
		image:            dbVersion.Spec.DB.Image,
		args:             args,
		cmd:              cmds,
		ports:            ports,
		envList:          envList,
		initContainers:   initContainers,
		gvrSvcName:       db.GoverningServiceName(),
		podTemplate:      &db.Spec.PodTemplate,
		configSecret:     db.Spec.ConfigSecret,
		pvcSpec:          db.Spec.Storage,
		replicas:         db.Spec.Replicas,
		volume:           volumes,
		volumeMount:      volumeMounts,
		monitorContainer: &monitorContainer,
	}

	return c.createOrPatchStatefulSet(db, opts)
}

func (c *Controller) checkStatefulSet(db *api.MariaDB, stsName string) error {
	// StatefulSet for MariaDB database
	statefulSet, err := c.Client.AppsV1().StatefulSets(db.Namespace).Get(context.TODO(), stsName, metav1.GetOptions{})
	if err != nil {
		if kerr.IsNotFound(err) {
			return nil
		}
		return err
	}

	if statefulSet.Labels[meta_util.NameLabelKey] != db.ResourceFQN() ||
		statefulSet.Labels[meta_util.InstanceLabelKey] != db.Name {
		return fmt.Errorf(`intended statefulSet "%v/%v" already exists`, db.Namespace, stsName)
	}

	return nil
}

func upsertCustomConfig(
	template core.PodTemplateSpec, configSecret *core.LocalObjectReference, replicas int32) core.PodTemplateSpec {
	for i, container := range template.Spec.Containers {
		if container.Name == api.ResourceSingularMariaDB {
			configVolumeMount := core.VolumeMount{
				Name: "custom-config",
				//MountPath: api.MariaDBCustomConfigMountPath,
				MountPath: "/etc/mysql/conf.d",
			}
			//if replicas > 1 {
			//	configVolumeMount.MountPath = api.MariaDBClusterCustomConfigMountPath
			//}
			volumeMounts := container.VolumeMounts
			volumeMounts = core_util.UpsertVolumeMount(volumeMounts, configVolumeMount)
			template.Spec.Containers[i].VolumeMounts = volumeMounts

			configVolume := core.Volume{
				Name: "custom-config",
				VolumeSource: core.VolumeSource{
					Secret: &core.SecretVolumeSource{
						SecretName: configSecret.Name,
					},
				},
			}

			volumes := template.Spec.Volumes
			volumes = core_util.UpsertVolume(volumes, configVolume)
			template.Spec.Volumes = volumes
			break
		}
	}

	return template
}

func (c *Controller) createOrPatchStatefulSet(db *api.MariaDB, opts workloadOptions) (kutil.VerbType, error) {
	// Take value of podTemplate
	var pt ofst.PodTemplateSpec
	if opts.podTemplate != nil {
		pt = *opts.podTemplate
	}
	// Create statefulSet for MariaDB database
	statefulSetMeta := metav1.ObjectMeta{
		Name:      opts.stsName,
		Namespace: db.Namespace,
	}

	owner := metav1.NewControllerRef(db, api.SchemeGroupVersion.WithKind(api.ResourceKindMariaDB))


	stsNew, vt, err := app_util.CreateOrPatchStatefulSet(
		context.TODO(),
		c.Client,
		statefulSetMeta,
		func(in *apps.StatefulSet) *apps.StatefulSet {
			in.Labels = opts.labels
			in.Annotations = pt.Controller.Annotations
			core_util.EnsureOwnerReference(&in.ObjectMeta, owner)

			in.Spec.Replicas = opts.replicas
			in.Spec.ServiceName = opts.gvrSvcName
			in.Spec.Selector = &metav1.LabelSelector{
				MatchLabels: opts.selectors,
			}
			in.Spec.Template.Labels = opts.selectors
			in.Spec.Template.Annotations = pt.Annotations
			in.Spec.Template.Spec.InitContainers = core_util.UpsertContainers(
				in.Spec.Template.Spec.InitContainers,
				pt.Spec.InitContainers,
			)
			in.Spec.Template.Spec.Containers = core_util.UpsertContainer(
				in.Spec.Template.Spec.Containers,
				core.Container{
					Name:            opts.conatainerName,
					Image:           opts.image,
					ImagePullPolicy: core.PullIfNotPresent,
					Command:         opts.cmd,
					Args:            opts.args,
					Ports:           opts.ports,
					Env:             core_util.UpsertEnvVars(opts.envList, pt.Spec.Env...),
					Resources:       pt.Spec.Resources,
					Lifecycle:       pt.Spec.Lifecycle,
					VolumeMounts:    opts.volumeMount,
				})

			in.Spec.Template.Spec.InitContainers = core_util.UpsertContainers(
				in.Spec.Template.Spec.InitContainers,
				opts.initContainers,
			)

			if opts.monitorContainer != nil && db.Spec.Monitor != nil && db.Spec.Monitor.Agent.Vendor() == mona.VendorPrometheus {
				in.Spec.Template.Spec.Containers = core_util.UpsertContainer(
					in.Spec.Template.Spec.Containers, *opts.monitorContainer)
			}

			in.Spec.Template.Spec.Volumes = core_util.UpsertVolume(in.Spec.Template.Spec.Volumes, opts.volume...)
			in = upsertEnv(in, db)
			in = upsertVolumes(in, db)

			if opts.configSecret != nil {
				in.Spec.Template = upsertCustomConfig(in.Spec.Template, opts.configSecret, pointer.Int32(db.Spec.Replicas))
			}

			in.Spec.Template.Spec.NodeSelector = pt.Spec.NodeSelector
			in.Spec.Template.Spec.Affinity = pt.Spec.Affinity
			if pt.Spec.SchedulerName != "" {
				in.Spec.Template.Spec.SchedulerName = pt.Spec.SchedulerName
			}
			in.Spec.Template.Spec.Tolerations = pt.Spec.Tolerations
			in.Spec.Template.Spec.ImagePullSecrets = pt.Spec.ImagePullSecrets
			in.Spec.Template.Spec.PriorityClassName = pt.Spec.PriorityClassName
			in.Spec.Template.Spec.Priority = pt.Spec.Priority
			in.Spec.Template.Spec.SecurityContext = pt.Spec.SecurityContext
			in.Spec.Template.Spec.ServiceAccountName = pt.Spec.ServiceAccountName
			in.Spec.UpdateStrategy = apps.StatefulSetUpdateStrategy{
				Type: apps.OnDeleteStatefulSetStrategyType,
			}

			in.Spec.Template.Spec.ReadinessGates = core_util.UpsertPodReadinessGateConditionType(in.Spec.Template.Spec.ReadinessGates, core_util.PodConditionTypeReady)

			return in
		},
		metav1.PatchOptions{},
	)

	if err != nil {
		return kutil.VerbUnchanged, err
	}

	if vt != kutil.VerbUnchanged {
		c.Recorder.Eventf(
			db,
			core.EventTypeNormal,
			eventer.EventReasonSuccessful,
			"Successfully %v StatefulSet %v/%v",
			vt, db.Namespace, opts.stsName,
		)
		if err := c.CreateStatefulSetPodDisruptionBudget(stsNew); err != nil{
			return kutil.VerbUnchanged, err
		}
		log.Info("successfully created/patched PodDisruptonBudget")
	}

	return vt, nil
}

func upsertVolumes(statefulSet *apps.StatefulSet, db *api.MariaDB) *apps.StatefulSet {
	//var volumeMounts []core.VolumeMount
	//var volume []core.Volume
	//var volumeClaimTemplates []core.PersistentVolumeClaim
	// Add DataVolume
	for i, container := range statefulSet.Spec.Template.Spec.Containers {
		if container.Name == api.ResourceSingularMariaDB {
			statefulSet.Spec.Template.Spec.Containers[i].VolumeMounts = core_util.UpsertVolumeMount(statefulSet.Spec.Template.Spec.Containers[i].VolumeMounts, core.VolumeMount{
				Name:      "data",
				MountPath: api.MariaDBDataMountPath,
			})
			pvcSpec := db.Spec.Storage
			if db.Spec.StorageType == api.StorageTypeEphemeral {
				ed := core.EmptyDirVolumeSource{}
				if pvcSpec != nil {
					if sz, found := pvcSpec.Resources.Requests[core.ResourceStorage]; found {
						ed.SizeLimit = &sz
					}
				}
				statefulSet.Spec.Template.Spec.Volumes = core_util.UpsertVolume(statefulSet.Spec.Template.Spec.Volumes, core.Volume{
					Name: "data",
					VolumeSource: core.VolumeSource{
						EmptyDir: &ed,
					},
				})
			} else {
				if len(pvcSpec.AccessModes) == 0 {
					pvcSpec.AccessModes = []core.PersistentVolumeAccessMode{
						core.ReadWriteOnce,
					}
					log.Infof(`Using "%v" as AccessModes in .spec.storage`, core.ReadWriteOnce)
				}

				claim := core.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name: "data",
					},
					Spec: *pvcSpec,
				}
				if pvcSpec.StorageClassName != nil {
					claim.Annotations = map[string]string{
						"volume.beta.kubernetes.io/storage-class": *pvcSpec.StorageClassName,
					}
				}
				statefulSet.Spec.VolumeClaimTemplates = core_util.UpsertVolumeClaim(statefulSet.Spec.VolumeClaimTemplates, claim)
			}
			break
		}
	}
	// upsert TLSConfig volumes
	if db.Spec.TLS != nil {
		statefulSet.Spec.Template.Spec.Volumes = core_util.UpsertVolume(
			statefulSet.Spec.Template.Spec.Volumes,
			[]core.Volume{
				{
					Name: "tls-server-config",
					VolumeSource: core.VolumeSource{
						Secret: &core.SecretVolumeSource{
							SecretName: db.MustCertSecretName(api.MariaDBServerCert),
							Items: []core.KeyToPath{
								{
									Key:  "ca.crt",
									Path: "ca.crt",
								},
								{
									Key:  "tls.crt",
									Path: "tls.crt",
								},
								{
									Key:  "tls.key",
									Path: "tls.key",
								},
							},
						},
					},
				},
				{
					Name: "tls-client-config",
					VolumeSource: core.VolumeSource{
						Secret: &core.SecretVolumeSource{
							SecretName: db.MustCertSecretName(api.MariaDBArchiverCert),
							Items: []core.KeyToPath{
								{
									Key:  "ca.crt",
									Path: "ca.crt",
								},
								{
									Key:  "tls.crt",
									Path: "tls.crt",
								},
								{
									Key:  "tls.key",
									Path: "tls.key",
								},
							},
						},
					},
				},
				{
					Name: "tls-exporter-config",
					VolumeSource: core.VolumeSource{
						Secret: &core.SecretVolumeSource{
							SecretName: db.MustCertSecretName(api.MariaDBMetricsExporterCert),
							Items: []core.KeyToPath{
								{
									Key:  "ca.crt",
									Path: "ca.crt",
								},
								{
									Key:  "tls.crt",
									Path: "tls.crt",
								},
								{
									Key:  "tls.key",
									Path: "tls.key",
								},
							},
						},
					},
				},
				{
					Name: "tls-metrics-exporter-config",
					VolumeSource: core.VolumeSource{
						Secret: &core.SecretVolumeSource{
							SecretName: meta_util.NameWithSuffix(db.Name, api.MySQLMetricsExporterConfigSecretSuffix),
							Items: []core.KeyToPath{
								{
									Key:  "exporter.cnf",
									Path: "exporter.cnf",
								},
							},
						},
					},
				},
			}...)

		for i, container := range statefulSet.Spec.Template.Spec.Containers {
			if container.Name == api.ResourceSingularMariaDB {
				statefulSet.Spec.Template.Spec.Containers[i].VolumeMounts = core_util.UpsertVolumeMount(statefulSet.Spec.Template.Spec.Containers[i].VolumeMounts,
					[]core.VolumeMount{
						{
							Name:      "tls-server-config",
							MountPath: "/etc/mysql/certs/server",
						},
						{
							Name:      "tls-client-config",
							MountPath: "/etc/mysql/certs/client",
						},
					}...)
			}
			if container.Name == api.ContainerExporterName {
				statefulSet.Spec.Template.Spec.Containers[i].VolumeMounts = core_util.UpsertVolumeMount(statefulSet.Spec.Template.Spec.Containers[i].VolumeMounts,
					[]core.VolumeMount{
						{
							Name:      "tls-exporter-config",
							MountPath: "/etc/mysql/certs/exporter",
						},
						{
							Name:      "tls-metrics-exporter-config",
							MountPath: "/etc/mysql/config/exporter",
						},
					}...)
			}
		}
	}

	return statefulSet
}

// upsertUserEnv add/overwrite env from user provided env in crd spec
func upsertEnv(statefulSet *apps.StatefulSet, db *api.MariaDB) *apps.StatefulSet {
	for i, container := range statefulSet.Spec.Template.Spec.Containers {
		if container.Name == api.ResourceSingularMariaDB || container.Name == "exporter" {
			envs := []core.EnvVar{
				{
					Name: "MYSQL_ROOT_PASSWORD",
					ValueFrom: &core.EnvVarSource{
						SecretKeyRef: &core.SecretKeySelector{
							LocalObjectReference: core.LocalObjectReference{
								Name: db.Spec.AuthSecret.Name,
							},
							Key: core.BasicAuthPasswordKey,
						},
					},
				},
				{
					Name: "MYSQL_ROOT_USERNAME",
					ValueFrom: &core.EnvVarSource{
						SecretKeyRef: &core.SecretKeySelector{
							LocalObjectReference: core.LocalObjectReference{
								Name: db.Spec.AuthSecret.Name,
							},
							Key: core.BasicAuthUsernameKey,
						},
					},
				},
			}

			statefulSet.Spec.Template.Spec.Containers[i].Env = core_util.UpsertEnvVars(container.Env, envs...)
		}
	}

	return statefulSet
}

func requiredSecretList(db *api.MariaDB) []string {
	var secretList []string
	for _, cert := range  db.Spec.TLS.Certificates {
		secretList = append(secretList,cert.SecretName)
	}

	if db.Spec.Monitor != nil {
		secretList = append(secretList, meta_util.NameWithSuffix(db.Name, api.MySQLMetricsExporterConfigSecretSuffix))
	}
	return secretList
}