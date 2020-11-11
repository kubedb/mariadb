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

package framework

import (
	"context"
	"fmt"
	"time"

	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"
	"kubedb.dev/apimachinery/client/clientset/versioned/typed/kubedb/v1alpha2/util"

	. "github.com/onsi/gomega"
	"gomodules.xyz/pointer"
	"gomodules.xyz/x/crypto/rand"
	core "k8s.io/api/core/v1"
	kerr "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	kutil "kmodules.xyz/client-go"
	meta_util "kmodules.xyz/client-go/meta"
)

var (
	JobPvcStorageSize = "50Mi"
	DBPvcStorageSize  = "50Mi"
)

func (f *Invocation) MariaDB() *api.MariaDB {
	return &api.MariaDB{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rand.WithUniqSuffix("mariadb"),
			Namespace: f.namespace,
			Labels: map[string]string{
				"app": f.app,
			},
		},
		Spec: api.MariaDBSpec{
			Replicas:    pointer.Int32P(1),
			Version:     DBCatalogName,
			StorageType: api.StorageTypeDurable,
			Storage: &core.PersistentVolumeClaimSpec{
				Resources: core.ResourceRequirements{
					Requests: core.ResourceList{
						core.ResourceStorage: resource.MustParse(DBPvcStorageSize),
					},
				},
				StorageClassName: pointer.StringP(f.StorageClass),
			},
			TerminationPolicy: api.TerminationPolicyHalt,
		},
	}
}

func (f *Invocation) MariaDBCluster() *api.MariaDB {
	mariadb := f.MariaDB()
	mariadb.Spec.Replicas = pointer.Int32P(api.MariaDBDefaultClusterSize)

	return mariadb
}

func (f *Framework) CreateMariaDB(obj *api.MariaDB) error {
	_, err := f.dbClient.KubedbV1alpha2().MariaDBs(obj.Namespace).Create(context.TODO(), obj, metav1.CreateOptions{})
	return err
}

func (f *Framework) GetMariaDB(meta metav1.ObjectMeta) (*api.MariaDB, error) {
	return f.dbClient.KubedbV1alpha2().MariaDBs(meta.Namespace).Get(context.TODO(), meta.Name, metav1.GetOptions{})
}

func (f *Framework) PatchMariaDB(meta metav1.ObjectMeta, transform func(*api.MariaDB) *api.MariaDB) (*api.MariaDB, error) {
	px, err := f.dbClient.KubedbV1alpha2().MariaDBs(meta.Namespace).Get(context.TODO(), meta.Name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	px, _, err = util.PatchMariaDB(context.TODO(), f.dbClient.KubedbV1alpha2(), px, transform, metav1.PatchOptions{})
	return px, err
}

func (f *Framework) DeleteMariaDB(meta metav1.ObjectMeta) error {
	return f.dbClient.KubedbV1alpha2().MariaDBs(meta.Namespace).Delete(context.TODO(), meta.Name, metav1.DeleteOptions{})
}

func (f *Framework) EventuallyMariaDB(meta metav1.ObjectMeta) GomegaAsyncAssertion {
	return Eventually(
		func() bool {
			_, err := f.dbClient.KubedbV1alpha2().MariaDBs(meta.Namespace).Get(context.TODO(), meta.Name, metav1.GetOptions{})
			if err != nil {
				if kerr.IsNotFound(err) {
					return false
				}
				Expect(err).NotTo(HaveOccurred())
			}
			return true
		},
		time.Minute*5,
		time.Second*5,
	)
}

func (f *Framework) EventuallyMariaDBPhase(meta metav1.ObjectMeta) GomegaAsyncAssertion {
	return Eventually(
		func() api.DatabasePhase {
			db, err := f.dbClient.KubedbV1alpha2().MariaDBs(meta.Namespace).Get(context.TODO(), meta.Name, metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred())
			return db.Status.Phase
		},
		time.Minute*5,
		time.Second*5,
	)
}

func (f *Framework) EventuallyMariaDBReady(meta metav1.ObjectMeta) GomegaAsyncAssertion {
	return Eventually(
		func() bool {
			px, err := f.dbClient.KubedbV1alpha2().MariaDBs(meta.Namespace).Get(context.TODO(), meta.Name, metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred())
			return px.Status.Phase == api.DatabasePhaseReady
		},
		time.Minute*5,
		time.Second*5,
	)
}

func (f *Framework) CleanMariaDB() {
	pxList, err := f.dbClient.KubedbV1alpha2().MariaDBs(f.namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return
	}
	for _, px := range pxList.Items {
		if _, _, err := util.PatchMariaDB(context.TODO(), f.dbClient.KubedbV1alpha2(), &px, func(in *api.MariaDB) *api.MariaDB {
			in.ObjectMeta.Finalizers = nil
			in.Spec.TerminationPolicy = api.TerminationPolicyWipeOut
			return in
		}, metav1.PatchOptions{}); err != nil {
			fmt.Printf("error Patching MariaDB. error: %v", err)
		}
	}
	if err := f.dbClient.KubedbV1alpha2().MariaDBs(f.namespace).DeleteCollection(context.TODO(), meta_util.DeleteInForeground(), metav1.ListOptions{}); err != nil {
		fmt.Printf("error in deletion of MariaDB. Error: %v", err)
	}
}

func (f *Framework) WaitUntilMariaDBReplicasBePatched(meta metav1.ObjectMeta, count int32) error {
	return wait.PollImmediate(kutil.RetryInterval, kutil.ReadinessTimeout, func() (bool, error) {
		px, err := f.GetMariaDB(meta)
		if err != nil {
			return false, nil
		}

		if *px.Spec.Replicas != count {
			return false, nil
		}

		return true, nil
	})
}
