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

package admission

import (
	"context"
	"net/http"
	"testing"

	catalog "kubedb.dev/apimachinery/apis/catalog/v1alpha1"
	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"
	extFake "kubedb.dev/apimachinery/client/clientset/versioned/fake"
	"kubedb.dev/apimachinery/client/clientset/versioned/scheme"

	"gomodules.xyz/pointer"
	admission "k8s.io/api/admission/v1beta1"
	authenticationV1 "k8s.io/api/authentication/v1"
	core "k8s.io/api/core/v1"
	storageV1beta1 "k8s.io/api/storage/v1beta1"
	kerr "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	clientSetScheme "k8s.io/client-go/kubernetes/scheme"
	"kmodules.xyz/client-go/meta"
	mona "kmodules.xyz/monitoring-agent-api/api/v1"
)

var requestKind = metaV1.GroupVersionKind{
	Group:   api.SchemeGroupVersion.Group,
	Version: api.SchemeGroupVersion.Version,
	Kind:    api.ResourceKindMariaDB,
}

func TestMariaDBValidator_Admit(t *testing.T) {
	if err := scheme.AddToScheme(clientSetScheme.Scheme); err != nil {
		t.Error(err)
	}
	for _, c := range cases {
		t.Run(c.testName, func(t *testing.T) {
			validator := MariaDBValidator{}

			validator.initialized = true
			validator.extClient = extFake.NewSimpleClientset(
				&catalog.MariaDBVersion{
					ObjectMeta: metaV1.ObjectMeta{
						Name: "10.5",
					},
					Spec: catalog.MariaDBVersionSpec{
						Version: "10.5",
					},
				},
				&catalog.MariaDBVersion{
					ObjectMeta: metaV1.ObjectMeta{
						Name: "10.4.17",
					},
					Spec: catalog.MariaDBVersionSpec{
						Version: "10.4.17",
					},
				},
			)
			validator.client = fake.NewSimpleClientset(
				&core.Secret{
					ObjectMeta: metaV1.ObjectMeta{
						Name:      "foo-auth",
						Namespace: "default",
					},
				},
				&storageV1beta1.StorageClass{
					ObjectMeta: metaV1.ObjectMeta{
						Name: "standard",
					},
				},
			)

			objJS, err := meta.MarshalToJson(&c.object, api.SchemeGroupVersion)
			if err != nil {
				panic(err)
			}
			oldObjJS, err := meta.MarshalToJson(&c.oldObject, api.SchemeGroupVersion)
			if err != nil {
				panic(err)
			}

			req := new(admission.AdmissionRequest)

			req.Kind = c.kind
			req.Name = c.objectName
			req.Namespace = c.namespace
			req.Operation = c.operation
			req.UserInfo = authenticationV1.UserInfo{}
			req.Object.Raw = objJS
			req.OldObject.Raw = oldObjJS

			if c.heatUp {
				if _, err := validator.extClient.KubedbV1alpha2().MariaDBs(c.namespace).Create(context.TODO(), &c.object, metaV1.CreateOptions{}); err != nil && !kerr.IsAlreadyExists(err) {
					t.Errorf(err.Error())
				}
			}
			if c.operation == admission.Delete {
				req.Object = runtime.RawExtension{}
			}
			if c.operation != admission.Update {
				req.OldObject = runtime.RawExtension{}
			}

			response := validator.Admit(req)
			if c.result == true {
				if response.Allowed != true {
					t.Errorf("expected: 'Allowed=true'. but got response: %v", response)
				}
			} else if c.result == false {
				if response.Allowed == true || response.Result.Code == http.StatusInternalServerError {
					t.Errorf("expected: 'Allowed=false', but got response: %v", response)
				}
			}
		})
	}

}

var cases = []struct {
	testName   string
	kind       metaV1.GroupVersionKind
	objectName string
	namespace  string
	operation  admission.Operation
	object     api.MariaDB
	oldObject  api.MariaDB
	heatUp     bool
	result     bool
}{
	{"Create Valid MariaDB",
		requestKind,
		"foo",
		"default",
		admission.Create,
		sampleMariaDB(),
		api.MariaDB{},
		false,
		true,
	},
	{"Create Invalid mariadb",
		requestKind,
		"foo",
		"default",
		admission.Create,
		getAwkwardMariaDB(),
		api.MariaDB{},
		false,
		false,
	},
	{"Edit MariaDB Spec.AuthSecret with Existing Secret",
		requestKind,
		"foo",
		"default",
		admission.Update,
		editExistingSecret(sampleMariaDB()),
		sampleMariaDB(),
		false,
		true,
	},
	{"Edit MariaDB Spec.AuthSecret with non Existing Secret",
		requestKind,
		"foo",
		"default",
		admission.Update,
		editNonExistingSecret(sampleMariaDB()),
		sampleMariaDB(),
		false,
		true,
	},
	{"Edit Status",
		requestKind,
		"foo",
		"default",
		admission.Update,
		editStatus(sampleMariaDB()),
		sampleMariaDB(),
		false,
		true,
	},
	{"Edit Spec.Monitor",
		requestKind,
		"foo",
		"default",
		admission.Update,
		editSpecMonitor(sampleMariaDB()),
		sampleMariaDB(),
		false,
		true,
	},
	{"Edit Invalid Spec.Monitor",
		requestKind,
		"foo",
		"default",
		admission.Update,
		editSpecInvalidMonitor(sampleMariaDB()),
		sampleMariaDB(),
		false,
		false,
	},
	{"Edit Spec.TerminationPolicy",
		requestKind,
		"foo",
		"default",
		admission.Update,
		pauseDatabase(sampleMariaDB()),
		sampleMariaDB(),
		false,
		true,
	},
	{"Delete MariaDB when Spec.TerminationPolicy=DoNotTerminate",
		requestKind,
		"foo",
		"default",
		admission.Delete,
		sampleMariaDB(),
		api.MariaDB{},
		true,
		false,
	},
	{"Delete MariaDB when Spec.TerminationPolicy=Pause",
		requestKind,
		"foo",
		"default",
		admission.Delete,
		pauseDatabase(sampleMariaDB()),
		api.MariaDB{},
		true,
		true,
	},
	{"Delete Non Existing MariaDB",
		requestKind,
		"foo",
		"default",
		admission.Delete,
		api.MariaDB{},
		api.MariaDB{},
		false,
		true,
	},

	// MariaDB Cluster
	{"Create a valid MariaDB Cluster",
		requestKind,
		"foo",
		"default",
		admission.Create,
		sampleValidMariaDBCluster(),
		api.MariaDB{},
		false,
		true,
	},
	{"Create an invalid MariaDB Cluster containing initscript",
		requestKind,
		"foo",
		"default",
		admission.Create,
		sampleMariaDBClusterContainingInitsript(),
		api.MariaDB{},
		false,
		false,
	},
	{"Create MariaDB Cluster with insufficient node replicas",
		requestKind,
		"foo",
		"default",
		admission.Create,
		insufficientNodeReplicas(),
		api.MariaDB{},
		false,
		false,
	},
	{"Create MariaDB Cluster with larger cluster name than recommended",
		requestKind,
		"foo",
		"default",
		admission.Create,
		largerClusterNameThanRecommended(),
		api.MariaDB{},
		false,
		false,
	},
	{"Edit spec.Init before provisioning complete",
		requestKind,
		"foo",
		"default",
		admission.Update,
		updateInit(sampleMariaDB()),
		sampleMariaDB(),
		true,
		true,
	},
	{"Edit spec.Init after provisioning complete",
		requestKind,
		"foo",
		"default",
		admission.Update,
		updateInit(completeInitialization(sampleMariaDB())),
		completeInitialization(sampleMariaDB()),
		true,
		false,
	},
}

func sampleMariaDB() api.MariaDB {
	return api.MariaDB{
		TypeMeta: metaV1.TypeMeta{
			Kind:       api.ResourceKindMariaDB,
			APIVersion: api.SchemeGroupVersion.String(),
		},
		ObjectMeta: metaV1.ObjectMeta{
			Name:      "foo",
			Namespace: "default",
			Labels: map[string]string{
				meta.NameLabelKey: api.MariaDB{}.ResourceFQN(),
			},
		},
		Spec: api.MariaDBSpec{
			Version:     "10.5",
			Replicas:    pointer.Int32P(1),
			StorageType: api.StorageTypeDurable,
			Storage: &core.PersistentVolumeClaimSpec{
				StorageClassName: pointer.StringP("standard"),
				Resources: core.ResourceRequirements{
					Requests: core.ResourceList{
						core.ResourceStorage: resource.MustParse("100Mi"),
					},
				},
			},
			Init: &api.InitSpec{
				WaitForInitialRestore: true,
			},
			TerminationPolicy: api.TerminationPolicyDoNotTerminate,
		},
	}
}

func getAwkwardMariaDB() api.MariaDB {
	px := sampleMariaDB()
	px.Spec.Version = "3.0"
	return px
}

func editExistingSecret(old api.MariaDB) api.MariaDB {
	old.Spec.AuthSecret = &core.LocalObjectReference{
		Name: "foo-auth",
	}
	return old
}

func editNonExistingSecret(old api.MariaDB) api.MariaDB {
	old.Spec.AuthSecret = &core.LocalObjectReference{
		Name: "foo-auth-fused",
	}
	return old
}

func editStatus(old api.MariaDB) api.MariaDB {
	old.Status = api.MariaDBStatus{
		Phase: api.DatabasePhaseReady,
	}
	return old
}

func editSpecMonitor(old api.MariaDB) api.MariaDB {
	old.Spec.Monitor = &mona.AgentSpec{
		Agent: mona.AgentPrometheusBuiltin,
		Prometheus: &mona.PrometheusSpec{
			Exporter: mona.PrometheusExporterSpec{
				Port: 1289,
			},
		},
	}
	return old
}

// should be failed because more fields required for COreOS Monitoring
func editSpecInvalidMonitor(old api.MariaDB) api.MariaDB {
	old.Spec.Monitor = &mona.AgentSpec{
		Agent: mona.AgentPrometheusOperator,
	}
	return old
}

func pauseDatabase(old api.MariaDB) api.MariaDB {
	old.Spec.TerminationPolicy = api.TerminationPolicyHalt
	return old
}

func sampleMariaDBClusterContainingInitsript() api.MariaDB {
	mariadb := sampleMariaDB()
	mariadb.Spec.Replicas = pointer.Int32P(api.MariaDBDefaultClusterSize)
	mariadb.Spec.Init = &api.InitSpec{
		Script: &api.ScriptSourceSpec{
			VolumeSource: core.VolumeSource{
				GitRepo: &core.GitRepoVolumeSource{
					Repository: "https://kubedb.dev/mariadb-init-scripts.git",
					Directory:  ".",
				},
			},
		},
	}

	return mariadb
}

func sampleValidMariaDBCluster() api.MariaDB {
	mariadb := sampleMariaDB()
	mariadb.Spec.Replicas = pointer.Int32P(api.MariaDBDefaultClusterSize)
	if mariadb.Spec.Init != nil {
		mariadb.Spec.Init.Script = nil
	}

	return mariadb
}

func insufficientNodeReplicas() api.MariaDB {
	mariadb := sampleValidMariaDBCluster()
	mariadb.Spec.Replicas = pointer.Int32P(2)

	return mariadb
}

func largerClusterNameThanRecommended() api.MariaDB {
	mariadb := sampleValidMariaDBCluster()
	mariadb.Name = "aaaaa-aaaaa-aaaaa-aaaaa-aaaaa-aaaaa"

	return mariadb
}

func completeInitialization(old api.MariaDB) api.MariaDB {
	old.Spec.Init.Initialized = true
	return old
}

func updateInit(old api.MariaDB) api.MariaDB {
	old.Spec.Init.WaitForInitialRestore = false
	return old
}
