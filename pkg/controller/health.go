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
	"strconv"
	"strings"
	"sync"

	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"
	"kubedb.dev/apimachinery/client/clientset/versioned/typed/kubedb/v1alpha2/util"

	_ "github.com/go-sql-driver/mysql"
	"github.com/go-xorm/xorm"
	"github.com/golang/glog"
	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	kmapi "kmodules.xyz/client-go/api/v1"
)

func (c *Controller) RunHealthChecker(stopCh <-chan struct{}) {
	// As CheckMySQLHealth() is a blocking function,
	// run it on a go-routine.
	go c.CheckMariaDBHealth(stopCh)
}

func (c *Controller) CheckMariaDBHealth(stopCh <-chan struct{}) {
	glog.Info("Starting MariDB health checker...")

	go wait.Until(func() {
		dbList, err := c.pxLister.MariaDBs(core.NamespaceAll).List(labels.Everything())
		if err != nil {
			glog.Errorf("Failed to list MariaDB objects with: %s", err.Error())
			return
		}

		var wg sync.WaitGroup
		for _, db := range dbList {
			wg.Add(1)
			go func() {
				defer func() {
					wg.Done()
				}()
				// Create database client
				engine, err := c.getMariaDBClient(db)
				if err != nil {
					// Since the client was unable to connect the database,
					// update "AcceptingConnection" to "false".
					// update "Ready" to "false"
					_, err = util.UpdateMariaDBStatus(
						context.TODO(),
						c.DBClient.KubedbV1alpha2(),
						db.ObjectMeta,
						func(in *api.MariaDBStatus) (types.UID, *api.MariaDBStatus) {
							in.Conditions = kmapi.SetCondition(in.Conditions,
								kmapi.Condition{
									Type:               api.DatabaseAcceptingConnection,
									Status:             core.ConditionFalse,
									Reason:             api.DatabaseNotAcceptingConnectionRequest,
									ObservedGeneration: db.Generation,
									Message:            fmt.Sprintf("The MariaDB: %s/%s is not accepting client requests, reason: %s", db.Namespace, db.Name, err.Error()),
								})
							return db.UID, in
						},
						metav1.UpdateOptions{},
					)
					if err != nil {
						glog.Errorf("Failed to update status for MariaDB: %s/%s", db.Namespace, db.Name)
					}
					// Since the client isn't created, skip rest operations.
					return
				}

				// While creating the client, we perform a health check along with it.
				// If the client is created without any error,
				// the database is accepting connection.
				// Update "AcceptingConnection" to "true".
				_, err = util.UpdateMariaDBStatus(
					context.TODO(),
					c.DBClient.KubedbV1alpha2(),
					db.ObjectMeta,
					func(in *api.MariaDBStatus) (types.UID, *api.MariaDBStatus) {
						in.Conditions = kmapi.SetCondition(in.Conditions,
							kmapi.Condition{
								Type:               api.DatabaseAcceptingConnection,
								Status:             core.ConditionTrue,
								Reason:             api.DatabaseAcceptingConnectionRequest,
								ObservedGeneration: db.Generation,
								Message:            fmt.Sprintf("The MariaDB: %s/%s is accepting client requests.", db.Namespace, db.Name),
							})
						return db.UID, in
					},
					metav1.UpdateOptions{},
				)
				if err != nil {
					glog.Errorf("Failed to update status for MariaDB: %s/%s", db.Namespace, db.Name)
					// Since condition update failed, skip remaining operations.
					return
				}
				// check MariaDB database health
				var isHealthy bool
				if *db.Spec.Replicas > int32(1) {
					isHealthy, err = c.checkMariaDBClusterHealth(db, engine)
					if err != nil {
						glog.Errorf("MariaDB Cluster %s/%s is not healthy, reason: %s", db.Namespace, db.Name, err.Error())
					}
				} else {
					isHealthy, err = c.checkMariaDBStandaloneHealth(engine)
					if err != nil {
						glog.Errorf("MariaDB standalone %s/%s is not healthy, reason: %s", db.Namespace, db.Name, err.Error())
					}
				}

				if !isHealthy {
					// Since the get status failed, skip remaining operations.
					return
				}
				// database is healthy. So update to "Ready" condition to "true"
				_, err = util.UpdateMariaDBStatus(
					context.TODO(),
					c.DBClient.KubedbV1alpha2(),
					db.ObjectMeta,
					func(in *api.MariaDBStatus) (types.UID, *api.MariaDBStatus) {
						in.Conditions = kmapi.SetCondition(in.Conditions,
							kmapi.Condition{
								Type:               api.DatabaseReady,
								Status:             core.ConditionTrue,
								Reason:             api.ReadinessCheckSucceeded,
								ObservedGeneration: db.Generation,
								Message:            fmt.Sprintf("The MySQL: %s/%s is ready.", db.Namespace, db.Name),
							})
						return db.UID, in
					},
					metav1.UpdateOptions{},
				)
				if err != nil {
					glog.Errorf("Failed to update status for MySQL: %s/%s", db.Namespace, db.Name)
				}

			}()
		}
		wg.Wait()
	}, c.ReadinessProbeInterval, stopCh)

	// will wait here until stopCh is closed.
	<-stopCh
	glog.Info("Shutting down MariaDB health checker...")
}

func (c *Controller) checkMariaDBClusterHealth(db *api.MariaDB, engine *xorm.Engine) (bool, error) {
	// sql queries for checking cluster healthiness

	// 1. ping database
	err := engine.Ping()
	if err != nil {
		return false, err
	}
	defer engine.Close()

	// 2. check all nodes are ONLINE
	result, err := engine.QueryString("SHOW STATUS LIKE 'wsrep_cluster_size';")
	if err != nil {
		return false, err
	}
	if result == nil {
		return false, fmt.Errorf("wsrep_cluster_size, query result is nil")
	}
	dbStatus, ok := result[0]["Value"]
	if !ok {
		return false, fmt.Errorf("can not read status from QueryString map")
	}
	if strings.Compare(dbStatus, strconv.Itoa(int(*db.Spec.Replicas))) != 0 {
		return false, fmt.Errorf("all group member are not online yet")
	}

	// 3. Internal Galera Cluster FSM state number.
	result, err = engine.QueryString("SHOW STATUS LIKE 'wsrep_local_state_comment';")
	if err != nil {
		return false, err
	}
	if result == nil {

		return false, fmt.Errorf("wsrep_local_state_comment, query result is nil")
	}
	dbStatus, ok = result[0]["Value"]
	if !ok {
		return false, fmt.Errorf("can not read status from QueryString map")
	}
	if strings.Compare(dbStatus, "Synced") != 0 {
		return false, fmt.Errorf("all group member are not synced")
	}

	// 4. Shows the internal state of the EVS Protocol.
	result, err = engine.QueryString("SHOW STATUS LIKE 'wsrep_evs_state';")
	if err != nil {
		return false, err
	}
	if result == nil {
		return false, fmt.Errorf("wsrep_evs_state, query result is nil")
	}
	dbStatus, ok = result[0]["Value"]
	if !ok {
		return false, fmt.Errorf("can not read status from QueryString map")
	}
	if strings.Compare(dbStatus, "OPERATIONAL") != 0 {
		return false, fmt.Errorf("wsrep_evs_state is not operational")
	}

	// 5.  Cluster component status. Possible values are PRIMARY (primary group configuration, quorum present),
	// NON_PRIMARY (non-primary group configuration, quorum lost) or DISCONNECTED (not connected to group, retrying).
	result, err = engine.QueryString("SHOW STATUS LIKE 'wsrep_cluster_status';")
	if err != nil {
		return false, err
	}
	if result == nil {
		return false, fmt.Errorf("wsrep_cluster_status, query result is nil")
	}
	dbStatus, ok = result[0]["Value"]
	if !ok {
		return false, fmt.Errorf("can not read status from QueryString map")
	}
	if strings.Compare(dbStatus, "Primary") != 0 {
		return false, fmt.Errorf("cluster component status is not primary")
	}

	// 6. Whether or not MariaDB is connected to the wsrep provider. Possible values are ON or OFF.
	result, err = engine.QueryString("SHOW STATUS LIKE 'wsrep_connected';")
	if err != nil {
		return false, err
	}
	if result == nil {
		return false, fmt.Errorf("wsrep_connected, query result is nil")
	}
	dbStatus, ok = result[0]["Value"]
	if !ok {
		return false, fmt.Errorf("can not read status from QueryString map")
	}
	if strings.Compare(dbStatus, "ON") != 0 {
		return false, fmt.Errorf("all group member are not connceted")
	}

	// 7. Whether or not the Galera wsrep provider is ready. Possible values are ON or OFF
	result, err = engine.QueryString("SHOW STATUS LIKE 'wsrep_ready';")
	if err != nil {
		return false, err
	}
	if result == nil {
		return false, fmt.Errorf("wsrep_ready, query result is nil")
	}
	dbStatus, ok = result[0]["Value"]
	if !ok {
		return false, fmt.Errorf("can not read status from QueryString map")
	}
	if strings.Compare(dbStatus, "ON") != 0 {
		return false, fmt.Errorf("galera wsrep provider is not ready")
	}

	return true, nil
}

func (c *Controller) checkMariaDBStandaloneHealth(engine *xorm.Engine) (bool, error) {
	// sql queries for checking standalone healthiness
	// 1. ping database
	err := engine.Ping()
	if err != nil {
		return false, err
	}
	return true, nil
}

func (c *Controller) getMariaDBClient(db *api.MariaDB) (*xorm.Engine, error) {
	port, err := c.GetPrimaryServicePort(db)
	if err != nil {
		return nil, err
	}

	user, pass, err := c.getMariaDBBasicAuth(db)
	if err != nil {
		return nil, fmt.Errorf("password basic auth for MariaDB %v/%v", db.Namespace, db.Name)
	}

	cnnstr := fmt.Sprintf("%v:%v@tcp(%s:%d)/%s", user, pass, getURL(db), port, "mysql")
	return xorm.NewEngine("mysql", cnnstr)
}

func (c *Controller) getMariaDBBasicAuth(db *api.MariaDB) (string, string, error) {
	var secretName string
	if db.Spec.AuthSecret != nil {
		secretName = db.GetAuthSecretName()
	}
	secret, err := c.Client.CoreV1().Secrets(db.Namespace).Get(context.TODO(), secretName, metav1.GetOptions{})
	if err != nil {
		return "", "", err
	}
	return string(secret.Data[core.BasicAuthUsernameKey]), string(secret.Data[core.BasicAuthPasswordKey]), nil
}

func getURL(db *api.MariaDB) string {
	return fmt.Sprintf("%s.%s.svc", db.GoverningServiceName(), db.GetNamespace())
}
