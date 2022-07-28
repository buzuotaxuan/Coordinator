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

    CoordinatorManager coordinatorManager=new CoordinatorManager(zookeeperProperties,true,
       "/test", new RandomTaskDistributionAlgorithm(),callback);

    coordinatorManager.init();

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

    CoordinatorManager coordinatorManager=new CoordinatorManager(zookeeperProperties,false,
        "/test", new RandomTaskDistributionAlgorithm(),callback);

    coordinatorManager.init();

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

    CoordinatorManager coordinatorManager=new CoordinatorManager(zookeeperProperties,true,
        "/test", new RandomTaskDistributionAlgorithm(),callback);

    coordinatorManager.init();

    Map map=new HashMap();
    map.put("id","test1");
    map.put("body","value");
    Task task=new Task("test1",map);
    coordinatorManager.addTask(task);


    TimeUnit.MINUTES.sleep(10);
    coordinatorManager.close();
  }

}
