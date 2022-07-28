package com.coordinator;

import com.coordinator.callback.TaskAssignmentCallback;
import com.coordinator.callback.TaskDistributedAlgorithm;
import com.coordinator.callback.ZooKeeperChildrenEventCallback;
import com.coordinator.config.ZookeeperProperties;
import com.coordinator.service.ZookeeperService;
import com.coordinator.util.IpUtil;
import com.coordinator.worker.WorkerLatch;
import com.coordinator.worker.tasks.Task;
import com.google.gson.Gson;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.zookeeper.CreateMode;

/**
 * @author Devy
 * @Description TODO
 * @createTime 2022年07月14日 16:50:00
 */
@Slf4j
public class CoordinatorManager {

  public CuratorFramework client;
  private String id;
  private WorkerLatch workerLatch;
  private String basePath;
  private static final Gson gson;
  static {
    gson = new Gson();
  }


  CoordinatorManager(ZookeeperProperties zookeeperProperties,
      String basePath
  ) throws Exception {
    this.id = IpUtil.getLocalIpByNetcard();
    this.client = ZookeeperService.init(zookeeperProperties.getZkServersIps(),
        zookeeperProperties.getNamespace(), zookeeperProperties.getUsername(),
        zookeeperProperties.getPassword());
    this.basePath=basePath;
  }


  public void registerMaster(TaskDistributedAlgorithm taskDistributedAlgorithm) throws Exception {
    register(true,taskDistributedAlgorithm,null);
  }

  public void registerWorker(
      TaskAssignmentCallback taskAssignmentCallback) throws Exception {
    register(false,null,taskAssignmentCallback);
  }

  public void register(boolean runForMaster, TaskDistributedAlgorithm taskDistributedAlgorithm,
      TaskAssignmentCallback taskAssignmentCallback) throws Exception {
    this.workerLatch = new WorkerLatch(this.id,this.basePath,client, runForMaster, taskDistributedAlgorithm,
        new ZooKeeperChildrenEventCallback() {
          @Override
          public void add(PathChildrenCacheEvent event) throws Exception {
            Task task = parseTask(event);
            taskAssignmentCallback.start(task);
          }

          @Override
          public void remove(PathChildrenCacheEvent event) throws Exception {
            Task task = parseTask(event);
            taskAssignmentCallback.stop(task);
          }

          @Override
          public void update(PathChildrenCacheEvent event) throws Exception {
            System.out.println("update");
          }
        });
    this.workerLatch.init();
  }

  private Task parseTask(PathChildrenCacheEvent event) throws Exception {
    String path = event.getData().getPath();
    String metaData = new String(client.getData().forPath(path), StandardCharsets.UTF_8);
    String id = StringUtils.substringAfterLast(path, "/");
    Task task = new Task();
    task.setId(id);
    task.setMetaMap(gson.fromJson(metaData, Map.class));
    task.setPath(path);
    return task;
  }

  public void close(){
    this.workerLatch.close();
  }


  public void addTask(Task task) throws Exception {
    log.debug( "Creating task for {} for scope {}", task);
    String path = this.basePath + "/tasks/"+ task.getId()+"-";
    client.create().withProtection().withMode(CreateMode.PERSISTENT_SEQUENTIAL ).inBackground().forPath( path, gson.toJson(task.getMetaMap()).getBytes(
        StandardCharsets.UTF_8));
  }


  public void updateTask(Task task) throws Exception {
    log.debug( "Creating task for {} for scope {}", task);
    client.setData().inBackground().forPath( task.getPath(), gson.toJson(task.getMetaMap()).getBytes(
        StandardCharsets.UTF_8));
  }

}
