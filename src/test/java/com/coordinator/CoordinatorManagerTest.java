package com.coordinator;

import com.coordinator.callback.TaskAssignmentCallback;
import com.coordinator.config.ZookeeperProperties;
import com.coordinator.worker.algo.RandomTaskDistributionAlgorithm;
import com.coordinator.worker.tasks.Task;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

/**
 * @author Devy
 * @Description TODO
 * @createTime 2022年07月21日 20:27:00
 */
public class CoordinatorManagerTest {

  @Test
  public void testLeaderStart() throws Exception {
    ZookeeperProperties zookeeperProperties=new ZookeeperProperties();
    List<String> zkServersIps = new ArrayList<>();
    zkServersIps.add( "127.0.0.1" );
    zookeeperProperties.setZkServersIps(zkServersIps);
    zookeeperProperties.setNamespace("coordinator");

    CoordinatorManager coordinatorManager=new CoordinatorManager(zookeeperProperties,
       "/test");

    coordinatorManager.registerMaster(new RandomTaskDistributionAlgorithm());

    TimeUnit.MINUTES.sleep(10);
    coordinatorManager.close();
  }

  @Test
  public void testWorkerStart() throws Exception {
    ZookeeperProperties zookeeperProperties=new ZookeeperProperties();
    List<String> zkServersIps = new ArrayList<>();
    zkServersIps.add( "127.0.0.1" );
    zookeeperProperties.setZkServersIps(zkServersIps);
    zookeeperProperties.setNamespace("coordinator");

    CoordinatorManager coordinatorManager =new CoordinatorManager(zookeeperProperties,
        "/test");

    TaskAssignmentCallback callback = new TaskAssignmentCallback() {
      @Override
      public void start(Task task) {
        System.out.println("start----"+task);
        new Thread(()->{
          try {
            TimeUnit.SECONDS.sleep(30);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          Map map= task.getMetaMap();
          map.put("body2","value2");
          try {
            coordinatorManager.updateTask(task);
          } catch (Exception exception) {
            exception.printStackTrace();
          }
        }).start();
      }

      @Override
      public void stop(Task task) {
        System.out.println("stop----"+task);
      }
    };
    coordinatorManager.registerWorker(callback);

    TimeUnit.MINUTES.sleep(10);
    coordinatorManager.close();
  }


  @Test
  public void addTask() throws Exception {
    ZookeeperProperties zookeeperProperties=new ZookeeperProperties();
    List<String> zkServersIps = new ArrayList<>();
    zkServersIps.add( "127.0.0.1" );
    zookeeperProperties.setZkServersIps(zkServersIps);
    zookeeperProperties.setNamespace("coordinator");
    TaskAssignmentCallback callback = new TaskAssignmentCallback() {
      @Override
      public void start(Task task) {
        System.out.println("start----"+task);
      }

      @Override
      public void stop(Task task) {
        System.out.println("stop----"+task);
      }
    };

    CoordinatorManager coordinatorManager=new CoordinatorManager(zookeeperProperties,
        "/test");

    coordinatorManager.registerMaster(new RandomTaskDistributionAlgorithm());

    Map map=new HashMap();
    map.put("id","test1");
    map.put("body","value");
    Task task=new Task();
    task.setId("test1");
    task.setMetaMap(map);
    coordinatorManager.addTask(task);


    TimeUnit.MINUTES.sleep(10);
    coordinatorManager.close();
  }

}
