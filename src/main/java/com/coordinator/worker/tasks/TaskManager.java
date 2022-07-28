package com.coordinator.worker.tasks;

import com.coordinator.callback.TaskDistributedAlgorithm;
import com.coordinator.constants.Constants;
import com.coordinator.worker.DistributionTask;
import com.google.common.base.Joiner;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.zookeeper.CreateMode;

/**
 * @author Devy
 * @Description TODO
 * @createTime 2022年07月15日 15:43:00
 */
@Slf4j
public class TaskManager {

  private  CuratorFramework client;
  private  TaskDistributedAlgorithm taskDistributedAlgorithm;
  private  String basePath;
  public  TaskManager(String basePath,CuratorFramework client, TaskDistributedAlgorithm taskDistributedAlgorithm){
    this.client=client;
    this.basePath=basePath;
    this.taskDistributedAlgorithm=taskDistributedAlgorithm;
    taskDistributedAlgorithm.init();
  }

  public void assignTask(CuratorCache workersCache, String task ) throws Exception {
    log.info("Assigning task {}", task);
    List<String> workersListPaths = new LinkedList<>();
    String workersPath=this.basePath + "/workers";
    List<ChildData> currentWorkers = workersCache.stream().filter(path->!workersPath.equals(path.getPath())).collect(Collectors.toList());
    if (currentWorkers.isEmpty()) {
      List <String> workers = client.getChildren().forPath(workersPath);
      workers.stream().forEach(worker ->{
        if(!worker.equals(workersPath)){
          workersListPaths.add(workersPath+ Constants.SLASH+worker);
        }

      });
    } else {

      for (ChildData workerChildData : currentWorkers) {
        workersListPaths.add(workerChildData.getPath());
      }
    }

    assignTaskByWorkers(task, workersListPaths);
  }

  private void assignTaskByWorkers(String task, List<String> workersListPaths) throws Exception {
    if (StringUtils.isEmpty(task) || workersListPaths == null || workersListPaths.size() == 0) {
      final String errorMessage = "Cannot assign task - illegal value for task  and/or workers list is null";
      log.error(errorMessage);
      throw new IllegalArgumentException(errorMessage);
    }

    boolean reassignTask = false;
    List<String> workersList = new LinkedList<>(workersListPaths);
    String targetWorker = null;

    do {
      try {
        DistributionTask distTask = new DistributionTask(basePath, task, workersList);
        targetWorker = taskDistributedAlgorithm.getTaskTargetWorker(distTask);
        if (StringUtils.isNotEmpty(targetWorker)) {
          reassignTask = true;
          String taskTargetPath = generateTaskAssignPath(targetWorker, task);
          moveTask(task, taskTargetPath);
          reassignTask = false;
        } else {
          reassignTask = false;
          final String errorMessage = "The task distribution algorithm returned a null or empty target worker for task " + generatePath(this.basePath, "tasks", task);
          log.error(errorMessage);
          throw new Exception(errorMessage);
        }
      } catch (Exception e) {
        final String workerFullPath = generatePath(this.basePath, "workers", targetWorker);
        final String taskFullPath = generatePath(this.basePath, "tasks", task);
        log.error("Could not assign task {} to worker {}", taskFullPath, workerFullPath);

        if (!reassignTask) {
          throw new Exception("CRITICAL: task " + taskFullPath + " failed to be assigned to a worker!");
        }

        if (!workersList.contains(workerFullPath)) {   // This should never happen, but is checked regardless to avoid an infinite loop
          log.error("The task's worker list was modified unexpectedly - not assigning the task {} to avoid an infinite loop", taskFullPath);
          throw new Exception("CRITICAL: task " + taskFullPath + " failed to be assigned to a worker!");
        }
        workersList.remove(workerFullPath);  // On the next iteration, the algorithm will select a new worker to try to assign to
      }
    } while (reassignTask && workersList.size() > 0);

    if (workersList.size() == 0) {   // Task failed to be assigned to all of the workers in the list
      final String criticalErrorMessage = "CRITICAL: task " + generatePath(this.basePath, "tasks", task) + " failed to be assigned to a worker!";
      log.error(criticalErrorMessage);
      throw new Exception(criticalErrorMessage);
    }
  }

  private void moveTask( String taskSourcePath, String taskDespPath ) throws Exception {
    String fullSourceTaskPath = generatePath(this.basePath, "tasks", taskSourcePath);
    byte[] taskData = client.getData().forPath( fullSourceTaskPath );
    log.info( "Moving task from {} to {}", fullSourceTaskPath, taskDespPath );
    client.inTransaction().create().withMode( CreateMode.PERSISTENT ).forPath( taskDespPath, taskData ).and().delete().forPath( fullSourceTaskPath ).and().commit();
    log.info( "Done moving task from {} to {}", fullSourceTaskPath, taskDespPath );
  }

  public String generateTaskAssignPath(String targetWorker, String task) {
    return generatePath(this.basePath, "assign", targetWorker, task);
  }

  public static String generatePath(String... nodes) {
    if (nodes == null || nodes.length == 0) {
      return null;
    }
    for (String node : nodes) {
      if (StringUtils.isEmpty(node)) {
        return null;
      }
    }
    // Scope is usually provided with root ('/') as the starting character - if not, add it as prefix
    String prefix = "";
    if (!(nodes[0].charAt(0) == '/')) {
      prefix = "/";
    }
    return prefix + Joiner.on("/").join(nodes);
  }


}
