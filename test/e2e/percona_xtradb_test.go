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

package e2e_test

import (
	"context"
	"fmt"
	"os"

	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"
	"kubedb.dev/apimachinery/client/clientset/versioned/typed/kubedb/v1alpha2/util"
	"kubedb.dev/mariadb/test/e2e/framework"
	"kubedb.dev/mariadb/test/e2e/matcher"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"gomodules.xyz/x/log"
	core "k8s.io/api/core/v1"
	kerr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kmapi "kmodules.xyz/client-go/api/v1"
	store "kmodules.xyz/objectstore-api/api/v1"
	stashV1alpha1 "stash.appscode.dev/apimachinery/apis/stash/v1alpha1"
	stashV1beta1 "stash.appscode.dev/apimachinery/apis/stash/v1beta1"
)

const (
	S3_BUCKET_NAME       = "S3_BUCKET_NAME"
	GCS_BUCKET_NAME      = "GCS_BUCKET_NAME"
	AZURE_CONTAINER_NAME = "AZURE_CONTAINER_NAME"
	SWIFT_CONTAINER_NAME = "SWIFT_CONTAINER_NAME"
	MYSQL_DATABASE       = "MYSQL_DATABASE"
	MYSQL_ROOT_PASSWORD  = "MYSQL_ROOT_PASSWORD"
)

const (
	googleProjectIDKey          = "GOOGLE_PROJECT_ID"
	googleServiceAccountJsonKey = "GOOGLE_SERVICE_ACCOUNT_JSON_KEY"
	googleBucketNameKey         = "GCS_BUCKET_NAME"
)

var _ = Describe("MariaDB", func() {
	var (
		err            error
		f              *framework.Invocation
		mariadb        *api.MariaDB
		garbageMariaDB *api.MariaDBList
		secret         *core.Secret
		skipMessage    string
		dbName         string
		dbNameKubedb   string
	)

	BeforeEach(func() {
		f = root.Invoke()
		mariadb = f.MariaDB()
		garbageMariaDB = new(api.MariaDBList)
		skipMessage = ""
		dbName = "mysql"
		dbNameKubedb = "kubedb"
	})

	var isSetEnv = func(key string) bool {
		_, set := os.LookupEnv(key)

		return set
	}

	var createAndWaitForRunning = func() {
		By("Create MariaDB: " + mariadb.Name)
		err = f.CreateMariaDB(mariadb)
		Expect(err).NotTo(HaveOccurred())

		By("Wait for Running MariaDB")
		f.EventuallyMariaDBReady(mariadb.ObjectMeta).Should(BeTrue())

		By("Wait for AppBinding to create")
		f.EventuallyAppBinding(mariadb.ObjectMeta).Should(BeTrue())

		By("Check valid AppBinding Specs")
		err := f.CheckAppBindingSpec(mariadb.ObjectMeta)
		Expect(err).NotTo(HaveOccurred())

		By("Waiting for database to be ready")
		f.EventuallyDatabaseReady(mariadb.ObjectMeta, dbName, 0).Should(BeTrue())
	}

	var create_Database_N_Table = func(meta metav1.ObjectMeta, podIndex int) {
		By("Create Database")
		f.EventuallyCreateDatabase(meta, dbName, podIndex).Should(BeTrue())

		By("Create Table")
		f.EventuallyCreateTable(meta, dbNameKubedb, podIndex).Should(BeTrue())
	}

	var countRows = func(meta metav1.ObjectMeta, podIndex, expectedRowCnt int) {
		By(fmt.Sprintf("Read row from member '%s-%d'", meta.Name, podIndex))
		f.EventuallyCountRow(meta, dbNameKubedb, podIndex).Should(Equal(expectedRowCnt))
	}

	var insertRows = func(meta metav1.ObjectMeta, podIndex, rowCntToInsert int) {
		By(fmt.Sprintf("Insert row on member '%s-%d'", meta.Name, podIndex))
		f.EventuallyInsertRow(meta, dbNameKubedb, podIndex, rowCntToInsert).Should(BeTrue())
	}

	var testGeneralBehaviour = func() {
		if skipMessage != "" {
			Skip(skipMessage)
		}
		// Create MariaDB
		createAndWaitForRunning()

		By("Creating Table")
		f.EventuallyCreateTable(mariadb.ObjectMeta, dbName, 0).Should(BeTrue())

		By("Inserting Rows")
		f.EventuallyInsertRow(mariadb.ObjectMeta, dbName, 0, 3).Should(BeTrue())

		By("Checking Row Count of Table")
		f.EventuallyCountRow(mariadb.ObjectMeta, dbName, 0).Should(Equal(3))

		By("Delete MariaDB")
		err = f.DeleteMariaDB(mariadb.ObjectMeta)
		Expect(err).NotTo(HaveOccurred())

		By("Wait for mariadb to be deleted")
		f.EventuallyMariaDB(mariadb.ObjectMeta).Should(BeFalse())

		// Create MariaDB object again to resume it
		By("Create MariaDB: " + mariadb.Name)
		err = f.CreateMariaDB(mariadb)
		Expect(err).NotTo(HaveOccurred())

		By("Wait for Running MariaDB")
		f.EventuallyMariaDBReady(mariadb.ObjectMeta).Should(BeTrue())

		By("Checking Row Count of Table")
		f.EventuallyCountRow(mariadb.ObjectMeta, dbName, 0).Should(Equal(3))

	}

	var shouldInsertData = func() {
		// Create and wait for running MariaDB
		createAndWaitForRunning()

		By("Creating Table")
		f.EventuallyCreateTable(mariadb.ObjectMeta, dbName, 0).Should(BeTrue())

		By("Inserting Row")
		f.EventuallyInsertRow(mariadb.ObjectMeta, dbName, 0, 3).Should(BeTrue())

		By("Checking Row Count of Table")
		f.EventuallyCountRow(mariadb.ObjectMeta, dbName, 0).Should(Equal(3))

		By("Create Secret")
		err := f.CreateSecret(secret)
		Expect(err).NotTo(HaveOccurred())

	}

	var deleteTestResource = func() {
		if mariadb == nil {
			log.Infoln("Skipping cleanup. Reason: MariaDB object is nil")
			return
		}

		By("Check if mariadb " + mariadb.Name + " exists.")
		px, err := f.GetMariaDB(mariadb.ObjectMeta)
		if err != nil && kerr.IsNotFound(err) {
			// MariaDB was not created. Hence, rest of cleanup is not necessary.
			return
		}
		Expect(err).NotTo(HaveOccurred())

		By("Update mariadb to set spec.terminationPolicy = WipeOut")
		_, err = f.PatchMariaDB(px.ObjectMeta, func(in *api.MariaDB) *api.MariaDB {
			in.Spec.TerminationPolicy = api.TerminationPolicyWipeOut
			return in
		})
		Expect(err).NotTo(HaveOccurred())

		By("Delete mariadb")
		err = f.DeleteMariaDB(mariadb.ObjectMeta)
		if err != nil && kerr.IsNotFound(err) {
			// MariaDB was not created. Hence, rest of cleanup is not necessary.
			return
		}
		Expect(err).NotTo(HaveOccurred())

		By("Wait for mariadb to be deleted")
		f.EventuallyMariaDB(mariadb.ObjectMeta).Should(BeFalse())

		By("Wait for mariadb resources to be wipedOut")
		f.EventuallyWipedOut(mariadb.ObjectMeta).Should(Succeed())
	}

	var deleteLeftOverStuffs = func() {
		// old MariaDB are in garbageMariaDB list. delete their resources.
		for _, p := range garbageMariaDB.Items {
			*mariadb = p
			deleteTestResource()
		}

		By("Delete left over workloads if exists any")
		f.CleanWorkloadLeftOvers()
	}

	AfterEach(func() {
		// delete resources for current MariaDB
		deleteTestResource()

		// old MariaDB are in garbageMariaDB list. delete their resources.
		for _, pc := range garbageMariaDB.Items {
			*mariadb = pc
			deleteTestResource()
		}

		By("Delete left over workloads if exists any")
		f.CleanWorkloadLeftOvers()
	})

	JustAfterEach(func() {
		if CurrentGinkgoTestDescription().Failed {
			f.PrintDebugHelpers(mariadb.Name, int(*mariadb.Spec.Replicas))
		}
	})

	Describe("Test", func() {
		Context("General", func() {
			Context("-", func() {
				It("should run successfully", testGeneralBehaviour)
			})
		})

		Context("Initialize", func() {
			Context("With Script", func() {
				var initScriptConfigmap *core.ConfigMap

				BeforeEach(func() {
					initScriptConfigmap, err = f.InitScriptConfigMap()
					Expect(err).ShouldNot(HaveOccurred())
					By("Create init Script ConfigMap: " + initScriptConfigmap.Name + "\n" + initScriptConfigmap.Data["init.sql"])
					Expect(f.CreateConfigMap(initScriptConfigmap)).ShouldNot(HaveOccurred())

					mariadb.Spec.Init = &api.InitSpec{
						Script: &api.ScriptSourceSpec{
							VolumeSource: core.VolumeSource{
								ConfigMap: &core.ConfigMapVolumeSource{
									LocalObjectReference: core.LocalObjectReference{
										Name: initScriptConfigmap.Name,
									},
								},
							},
						},
					}
				})

				AfterEach(func() {
					By("Deleting configMap: " + initScriptConfigmap.Name)
					Expect(f.DeleteConfigMap(initScriptConfigmap.ObjectMeta)).NotTo(HaveOccurred())
				})

				It("should run successfully", func() {
					// Create MariaDB
					createAndWaitForRunning()

					By("Checking Row Count of Table")
					f.EventuallyCountRow(mariadb.ObjectMeta, dbName, 0).Should(Equal(3))
				})
			})

			// To run this test,
			// 1st: Deploy stash latest operator
			// 2nd: create mysql related tasks and functions either
			//	 or	from helm chart in `stash.appscode.dev/mariadb/chart/stash-mariadb`
			Context("With Stash/Restic", func() {
				var bc *stashV1beta1.BackupConfiguration
				var bs *stashV1beta1.BackupSession
				var rs *stashV1beta1.RestoreSession
				var repo *stashV1alpha1.Repository

				BeforeEach(func() {
					if !f.FoundStashCRDs() {
						Skip("Skipping tests for stash integration. reason: stash operator is not running.")
					}

					if !isSetEnv(googleProjectIDKey) ||
						!isSetEnv(googleServiceAccountJsonKey) ||
						!isSetEnv(googleBucketNameKey) {

						Skip("Skipping tests for stash integration. reason: " +
							fmt.Sprintf("env vars %q, %q and %q are required",
								googleProjectIDKey, googleServiceAccountJsonKey, googleBucketNameKey))
					}
				})

				AfterEach(func() {
					By("Deleting BackupConfiguration")
					err := f.DeleteBackupConfiguration(bc.ObjectMeta)
					Expect(err).NotTo(HaveOccurred())

					By("Deleting RestoreSessionForCluster")
					err = f.DeleteRestoreSession(rs.ObjectMeta)
					Expect(err).NotTo(HaveOccurred())

					By("Deleting Repository")
					err = f.DeleteRepository(repo.ObjectMeta)
					Expect(err).NotTo(HaveOccurred())

					deleteTestResource()
					deleteLeftOverStuffs()
				})

				var createAndWaitForInitializing = func() {
					By("Creating MariaDB: " + mariadb.Name)
					err = f.CreateMariaDB(mariadb)
					Expect(err).NotTo(HaveOccurred())

					By("Wait for restoring mariadb")
					f.EventuallyMariaDBPhase(mariadb.ObjectMeta).Should(Equal(api.DatabasePhaseDataRestoring))
				}

				var shouldInitializeFromStash = func() {
					// Create and wait for running MySQL
					createAndWaitForRunning()

					create_Database_N_Table(mariadb.ObjectMeta, 0)
					insertRows(mariadb.ObjectMeta, 0, 3)
					countRows(mariadb.ObjectMeta, 0, 3)

					By("Create Secret")
					err = f.CreateSecret(secret)
					Expect(err).NotTo(HaveOccurred())

					By("Create Repositories")
					err = f.CreateRepository(repo)
					Expect(err).NotTo(HaveOccurred())

					By("Create BackupConfiguration")
					err = f.CreateBackupConfiguration(bc)
					Expect(err).NotTo(HaveOccurred())

					By("Wait until BackupSession be created")
					bs, err = f.WaitUntilBackkupSessionBeCreated(bc.ObjectMeta)

					// eventually backupsession succeeded
					By("Check for Succeeded backupsession")
					f.EventuallyBackupSessionPhase(bs.ObjectMeta).Should(Equal(stashV1beta1.BackupSessionSucceeded))

					oldMariaDB, err := f.GetMariaDB(mariadb.ObjectMeta)
					Expect(err).NotTo(HaveOccurred())

					garbageMariaDB.Items = append(garbageMariaDB.Items, *oldMariaDB)

					By("Create MariaDB for initializing from stash")
					*mariadb = *f.MariaDB()
					rs = f.RestoreSessionForStandalone(mariadb.ObjectMeta, oldMariaDB.ObjectMeta)
					mariadb.Spec.AuthSecret = oldMariaDB.Spec.AuthSecret
					mariadb.Spec.Init = &api.InitSpec{
						WaitForInitialRestore: true,
					}

					// Create and wait for running MySQL
					createAndWaitForInitializing()

					By("Create RestoreSessionForCluster")
					err = f.CreateRestoreSession(rs)
					Expect(err).NotTo(HaveOccurred())

					// eventually restoresession succeeded
					By("Check for Succeeded restoreSession")
					f.EventuallyRestoreSessionPhase(rs.ObjectMeta).Should(Equal(stashV1beta1.RestoreSucceeded))

					By("Wait for Running mariadb")
					f.EventuallyMariaDBReady(mariadb.ObjectMeta).Should(BeTrue())

					By("Wait for AppBinding to create")
					f.EventuallyAppBinding(mariadb.ObjectMeta).Should(BeTrue())

					By("Check valid AppBinding Specs")
					err = f.CheckAppBindingSpec(mariadb.ObjectMeta)
					Expect(err).NotTo(HaveOccurred())

					By("Waiting for database to be ready")
					f.EventuallyDatabaseReady(mariadb.ObjectMeta, dbName, 0).Should(BeTrue())

					countRows(mariadb.ObjectMeta, 0, 3)
				}

				Context("From GCS backend", func() {

					BeforeEach(func() {
						secret = f.SecretForGCSBackend()
						secret = f.PatchSecretForRestic(secret)
						bc = f.BackupConfiguration(mariadb.ObjectMeta)
						repo = f.Repository(mariadb.ObjectMeta, secret.Name)

						repo.Spec.Backend = store.Backend{
							GCS: &store.GCSSpec{
								Bucket: os.Getenv(googleBucketNameKey),
								Prefix: fmt.Sprintf("stash/%v/%v", mariadb.Namespace, mariadb.Name),
							},
							StorageSecretName: secret.Name,
						}
					})

					It("should run successfully", shouldInitializeFromStash)
				})
			})
		})

		Context("Resume", func() {
			Context("Super Fast User - Create-Delete-Create-Delete-Create ", func() {
				It("should resume DormantDatabase successfully", func() {
					// Create and wait for running MariaDB
					createAndWaitForRunning()

					By("Creating Table")
					f.EventuallyCreateTable(mariadb.ObjectMeta, dbName, 0).Should(BeTrue())

					By("Inserting Row")
					f.EventuallyInsertRow(mariadb.ObjectMeta, dbName, 0, 3).Should(BeTrue())

					By("Checking Row Count of Table")
					f.EventuallyCountRow(mariadb.ObjectMeta, dbName, 0).Should(Equal(3))

					By("Delete MariaDB")
					err = f.DeleteMariaDB(mariadb.ObjectMeta)
					Expect(err).NotTo(HaveOccurred())

					By("Wait for mariadb to be deleted")
					f.EventuallyMariaDB(mariadb.ObjectMeta).Should(BeFalse())

					// Create MariaDB object again to resume it
					By("Create MariaDB: " + mariadb.Name)
					err = f.CreateMariaDB(mariadb)
					Expect(err).NotTo(HaveOccurred())

					// Delete without caring if DB is resumed
					By("Delete MariaDB")
					err = f.DeleteMariaDB(mariadb.ObjectMeta)
					Expect(err).NotTo(HaveOccurred())

					By("Wait for MariaDB to be deleted")
					f.EventuallyMariaDB(mariadb.ObjectMeta).Should(BeFalse())

					// Create MariaDB object again to resume it
					By("Create MariaDB: " + mariadb.Name)
					err = f.CreateMariaDB(mariadb)
					Expect(err).NotTo(HaveOccurred())

					By("Wait for Running MariaDB")
					f.EventuallyMariaDBReady(mariadb.ObjectMeta).Should(BeTrue())

					By("Checking Row Count of Table")
					f.EventuallyCountRow(mariadb.ObjectMeta, dbName, 0).Should(Equal(3))
				})
			})

			Context("Without Init", func() {
				It("should resume DormantDatabase successfully", func() {
					// Create and wait for running MariaDB
					createAndWaitForRunning()

					By("Creating Table")
					f.EventuallyCreateTable(mariadb.ObjectMeta, dbName, 0).Should(BeTrue())

					By("Inserting Row")
					f.EventuallyInsertRow(mariadb.ObjectMeta, dbName, 0, 3).Should(BeTrue())

					By("Checking Row Count of Table")
					f.EventuallyCountRow(mariadb.ObjectMeta, dbName, 0).Should(Equal(3))

					By("Delete MariaDB")
					err = f.DeleteMariaDB(mariadb.ObjectMeta)
					Expect(err).NotTo(HaveOccurred())

					By("Wait for mariadb to be deleted")
					f.EventuallyMariaDB(mariadb.ObjectMeta).Should(BeFalse())

					// Create MariaDB object again to resume it
					By("Create MariaDB: " + mariadb.Name)
					err = f.CreateMariaDB(mariadb)
					Expect(err).NotTo(HaveOccurred())

					By("Wait for Running MariaDB")
					f.EventuallyMariaDBReady(mariadb.ObjectMeta).Should(BeTrue())

					By("Checking Row Count of Table")
					f.EventuallyCountRow(mariadb.ObjectMeta, dbName, 0).Should(Equal(3))
				})
			})

			Context("with init Script", func() {
				var initScriptConfigmap *core.ConfigMap

				BeforeEach(func() {
					initScriptConfigmap, err = f.InitScriptConfigMap()
					Expect(err).ShouldNot(HaveOccurred())
					By("Create init Script ConfigMap: " + initScriptConfigmap.Name + "\n" + initScriptConfigmap.Data["init.sql"])
					Expect(f.CreateConfigMap(initScriptConfigmap)).ShouldNot(HaveOccurred())

					mariadb.Spec.Init = &api.InitSpec{
						Script: &api.ScriptSourceSpec{
							VolumeSource: core.VolumeSource{
								ConfigMap: &core.ConfigMapVolumeSource{
									LocalObjectReference: core.LocalObjectReference{
										Name: initScriptConfigmap.Name,
									},
								},
							},
						},
					}
				})

				AfterEach(func() {
					By("Deleting configMap: " + initScriptConfigmap.Name)
					Expect(f.DeleteConfigMap(initScriptConfigmap.ObjectMeta)).NotTo(HaveOccurred())
				})

				It("should resume DormantDatabase successfully", func() {
					// Create and wait for running MariaDB
					createAndWaitForRunning()

					By("Checking Row Count of Table")
					f.EventuallyCountRow(mariadb.ObjectMeta, dbName, 0).Should(Equal(3))

					By("Delete MariaDB")
					err = f.DeleteMariaDB(mariadb.ObjectMeta)
					Expect(err).NotTo(HaveOccurred())

					By("Wait for mariadb to be deleted")
					f.EventuallyMariaDB(mariadb.ObjectMeta).Should(BeFalse())

					// Create MariaDB object again to resume it
					By("Create MariaDB: " + mariadb.Name)
					err = f.CreateMariaDB(mariadb)
					Expect(err).NotTo(HaveOccurred())

					By("Wait for Running MariaDB")
					f.EventuallyMariaDBReady(mariadb.ObjectMeta).Should(BeTrue())

					By("Checking Row Count of Table")
					f.EventuallyCountRow(mariadb.ObjectMeta, dbName, 0).Should(Equal(3))

					mariadb, err := f.GetMariaDB(mariadb.ObjectMeta)
					Expect(err).NotTo(HaveOccurred())
					Expect(mariadb.Spec.Init).NotTo(BeNil())

					By("Checking MariaDB crd does not have DataRestored condition")
					Expect(kmapi.HasCondition(mariadb.Status.Conditions, api.DatabaseDataRestored)).To(BeFalse())
				})
			})

			Context("Multiple times with init", func() {
				var initScriptConfigmap *core.ConfigMap

				BeforeEach(func() {
					initScriptConfigmap, err = f.InitScriptConfigMap()
					Expect(err).ShouldNot(HaveOccurred())
					By("Create init Script ConfigMap: " + initScriptConfigmap.Name + "\n" + initScriptConfigmap.Data["init.sql"])
					Expect(f.CreateConfigMap(initScriptConfigmap)).ShouldNot(HaveOccurred())

					mariadb.Spec.Init = &api.InitSpec{
						Script: &api.ScriptSourceSpec{
							VolumeSource: core.VolumeSource{
								ConfigMap: &core.ConfigMapVolumeSource{
									LocalObjectReference: core.LocalObjectReference{
										Name: initScriptConfigmap.Name,
									},
								},
							},
						},
					}
				})

				AfterEach(func() {
					By("Deleting configMap: " + initScriptConfigmap.Name)
					Expect(f.DeleteConfigMap(initScriptConfigmap.ObjectMeta)).NotTo(HaveOccurred())
				})

				It("should resume DormantDatabase successfully", func() {
					// Create and wait for running MariaDB
					createAndWaitForRunning()

					By("Checking Row Count of Table")
					f.EventuallyCountRow(mariadb.ObjectMeta, dbName, 0).Should(Equal(3))

					for i := 0; i < 3; i++ {
						By(fmt.Sprintf("%v-th", i+1) + " time running.")

						By("Delete MariaDB")
						err = f.DeleteMariaDB(mariadb.ObjectMeta)
						Expect(err).NotTo(HaveOccurred())

						By("Wait for mariadb to be deleted")
						f.EventuallyMariaDB(mariadb.ObjectMeta).Should(BeFalse())

						// Create MariaDB object again to resume it
						By("Create MariaDB: " + mariadb.Name)
						err = f.CreateMariaDB(mariadb)
						Expect(err).NotTo(HaveOccurred())

						By("Wait for Running MariaDB")
						f.EventuallyMariaDBReady(mariadb.ObjectMeta).Should(BeTrue())

						By("Checking Row Count of Table")
						f.EventuallyCountRow(mariadb.ObjectMeta, dbName, 0).Should(Equal(3))

						mariadb, err := f.GetMariaDB(mariadb.ObjectMeta)
						Expect(err).NotTo(HaveOccurred())
						Expect(mariadb.Spec.Init).ShouldNot(BeNil())

						By("Checking MariaDB crd does not have DataRestored condition")
						Expect(kmapi.HasCondition(mariadb.Status.Conditions, api.DatabaseDataRestored)).To(BeFalse())
					}
				})
			})
		})

		Context("Termination Policy", func() {

			BeforeEach(func() {
				//skipDataChecking = false
				secret = f.SecretForGCSBackend()
				//snapshot.Spec.StorageSecretName = secret.Name
				//snapshot.Spec.GCS = &store.GCSSpec{
				//	Bucket: os.Getenv(GCS_BUCKET_NAME),
				//}
				//snapshot.Spec.DatabaseName = mariadb.Name
			})

			Context("with TerminationDoNotTerminate", func() {
				BeforeEach(func() {
					//skipDataChecking = true
					mariadb.Spec.TerminationPolicy = api.TerminationPolicyDoNotTerminate
				})

				It("should work successfully", func() {
					// Create and wait for running MariaDB
					createAndWaitForRunning()

					By("Delete MariaDB")
					err = f.DeleteMariaDB(mariadb.ObjectMeta)
					Expect(err).Should(HaveOccurred())

					By("MariaDB is not paused. Check for MariaDB")
					f.EventuallyMariaDB(mariadb.ObjectMeta).Should(BeTrue())

					By("Check for Running MariaDB")
					f.EventuallyMariaDBReady(mariadb.ObjectMeta).Should(BeTrue())

					By("Update MariaDB to set spec.terminationPolicy = Pause")
					_, err := f.PatchMariaDB(mariadb.ObjectMeta, func(in *api.MariaDB) *api.MariaDB {
						in.Spec.TerminationPolicy = api.TerminationPolicyHalt
						return in
					})
					Expect(err).NotTo(HaveOccurred())
				})
			})

			Context("with TerminationPolicyHalt (default)", func() {

				AfterEach(func() {
					By("Deleting secret: " + secret.Name)
					err := f.DeleteSecret(secret.ObjectMeta)
					if err != nil && !kerr.IsNotFound(err) {
						Expect(err).NotTo(HaveOccurred())
					}
				})

				It("should create DormantDatabase and resume from it", func() {
					// Run MariaDB and take snapshot
					shouldInsertData()

					By("Deleting MariaDB crd")
					err = f.DeleteMariaDB(mariadb.ObjectMeta)
					Expect(err).NotTo(HaveOccurred())

					By("Wait for mariadb to be deleted")
					f.EventuallyMariaDB(mariadb.ObjectMeta).Should(BeFalse())

					By("Checking PVC hasn't been deleted")
					f.EventuallyPVCCount(mariadb.ObjectMeta).Should(Equal(1))

					By("Checking Secret hasn't been deleted")
					f.EventuallyDBSecretCount(mariadb.ObjectMeta).Should(Equal(1))

					// Create MariaDB object again to resume it
					By("Create (resume) MariaDB: " + mariadb.Name)
					err = f.CreateMariaDB(mariadb)
					Expect(err).NotTo(HaveOccurred())

					By("Wait for Running MariaDB")
					f.EventuallyMariaDBReady(mariadb.ObjectMeta).Should(BeTrue())

					By("Checking row count of table")
					f.EventuallyCountRow(mariadb.ObjectMeta, dbName, 0).Should(Equal(3))
				})
			})

			Context("with TerminationPolicyDelete", func() {

				BeforeEach(func() {
					mariadb.Spec.TerminationPolicy = api.TerminationPolicyDelete
				})

				AfterEach(func() {
					By("Deleting secret: " + secret.Name)
					err := f.DeleteSecret(secret.ObjectMeta)
					if err != nil && !kerr.IsNotFound(err) {
						Expect(err).NotTo(HaveOccurred())
					}
				})

				It("should not create DormantDatabase and should not delete secret", func() {
					// Run MariaDB and take snapshot
					shouldInsertData()

					By("Delete MariaDB")
					err = f.DeleteMariaDB(mariadb.ObjectMeta)
					Expect(err).NotTo(HaveOccurred())

					By("wait until MariaDB is deleted")
					f.EventuallyMariaDB(mariadb.ObjectMeta).Should(BeFalse())

					By("Checking PVC has been deleted")
					f.EventuallyPVCCount(mariadb.ObjectMeta).Should(Equal(0))

					By("Checking Secret hasn't been deleted")
					f.EventuallyDBSecretCount(mariadb.ObjectMeta).Should(Equal(1))
				})
			})

			Context("with TerminationPolicyWipeOut", func() {

				BeforeEach(func() {
					mariadb.Spec.TerminationPolicy = api.TerminationPolicyWipeOut
				})

				It("should not create DormantDatabase and should wipeOut all", func() {
					// Run MariaDB and take snapshot
					shouldInsertData()

					By("Delete MariaDB")
					err = f.DeleteMariaDB(mariadb.ObjectMeta)
					Expect(err).NotTo(HaveOccurred())

					By("wait until MariaDB is deleted")
					f.EventuallyMariaDB(mariadb.ObjectMeta).Should(BeFalse())

					By("Checking PVCs has been deleted")
					f.EventuallyPVCCount(mariadb.ObjectMeta).Should(Equal(0))
				})
			})
		})

		Context("EnvVars", func() {
			Context("Database Name as EnvVar", func() {
				It("should create DB with name provided in EvnVar", func() {
					if skipMessage != "" {
						Skip(skipMessage)
					}

					dbName = f.App()
					mariadb.Spec.PodTemplate.Spec.Env = []core.EnvVar{
						{
							Name:  MYSQL_DATABASE,
							Value: dbName,
						},
					}
					//test general behaviour
					testGeneralBehaviour()
				})
			})

			Context("Root Password as EnvVar", func() {
				It("should reject to create MariaDB CRD", func() {
					if skipMessage != "" {
						Skip(skipMessage)
					}

					mariadb.Spec.PodTemplate.Spec.Env = []core.EnvVar{
						{
							Name:  MYSQL_ROOT_PASSWORD,
							Value: "not@secret",
						},
					}
					By("Create MariaDB: " + mariadb.Name)
					err = f.CreateMariaDB(mariadb)
					Expect(err).To(HaveOccurred())
				})
			})

			Context("Update EnvVar", func() {
				It("should not reject to update EvnVar", func() {
					if skipMessage != "" {
						Skip(skipMessage)
					}

					dbName = f.App()
					mariadb.Spec.PodTemplate.Spec.Env = []core.EnvVar{
						{
							Name:  MYSQL_DATABASE,
							Value: dbName,
						},
					}
					//test general behaviour
					testGeneralBehaviour()

					By("Patching EnvVar")
					_, _, err = util.PatchMariaDB(context.TODO(), f.ExtClient().KubedbV1alpha2(), mariadb, func(in *api.MariaDB) *api.MariaDB {
						in.Spec.PodTemplate.Spec.Env = []core.EnvVar{
							{
								Name:  MYSQL_DATABASE,
								Value: "patched-db",
							},
						}
						return in
					}, metav1.PatchOptions{})
					Expect(err).NotTo(HaveOccurred())
				})
			})
		})

		Context("Custom config", func() {

			customConfigs := []string{
				"max_connections=200",
				"read_buffer_size=1048576", // 1MB
			}

			Context("from configMap", func() {
				var userConfig *core.Secret

				BeforeEach(func() {
					userConfig = f.GetCustomConfig(customConfigs)
				})

				AfterEach(func() {
					By("Deleting secret: " + userConfig.Name)
					err := f.DeleteSecret(userConfig.ObjectMeta)
					Expect(err).NotTo(HaveOccurred())

				})

				It("should set configuration provided in configMap", func() {
					if skipMessage != "" {
						Skip(skipMessage)
					}

					By("Creating configMap: " + userConfig.Name)
					err := f.CreateSecret(userConfig)
					Expect(err).NotTo(HaveOccurred())

					mariadb.Spec.ConfigSecret = &core.LocalObjectReference{
						Name: userConfig.Name,
					}

					// Create MariaDB
					createAndWaitForRunning()

					By("Checking MariaDB configured from provided custom configuration")
					for _, cfg := range customConfigs {
						f.EventuallyMariaDBVariable(mariadb.ObjectMeta, dbName, 0, cfg).Should(matcher.UseCustomConfig(cfg))
					}
				})
			})
		})

		Context("StorageType ", func() {
			var shouldRunSuccessfully = func() {
				if skipMessage != "" {
					Skip(skipMessage)
				}

				// Create MariaDB
				createAndWaitForRunning()

				By("Creating Table")
				f.EventuallyCreateTable(mariadb.ObjectMeta, dbName, 0).Should(BeTrue())

				By("Inserting Rows")
				f.EventuallyInsertRow(mariadb.ObjectMeta, dbName, 0, 3).Should(BeTrue())

				By("Checking Row Count of Table")
				f.EventuallyCountRow(mariadb.ObjectMeta, dbName, 0).Should(Equal(3))
			}

			Context("Ephemeral", func() {
				Context("General Behaviour", func() {
					BeforeEach(func() {
						mariadb.Spec.StorageType = api.StorageTypeEphemeral
						mariadb.Spec.Storage = nil
						mariadb.Spec.TerminationPolicy = api.TerminationPolicyWipeOut
					})

					It("should run successfully", shouldRunSuccessfully)
				})

				Context("With TerminationPolicyHalt", func() {
					BeforeEach(func() {
						mariadb.Spec.StorageType = api.StorageTypeEphemeral
						mariadb.Spec.Storage = nil
						mariadb.Spec.TerminationPolicy = api.TerminationPolicyHalt
					})

					It("should reject to create MariaDB object", func() {

						By("Creating MariaDB: " + mariadb.Name)
						err := f.CreateMariaDB(mariadb)
						Expect(err).To(HaveOccurred())
					})
				})
			})
		})
	})
})
