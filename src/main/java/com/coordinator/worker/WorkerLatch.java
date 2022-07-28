package com.coordinator.worker;

import com.coordinator.callback.TaskDistributedAlgorithm;
import com.coordinator.callback.ZooKeeperChildrenEventCallback;
import com.coordinator.constants.Constants;
import com.coordinator.exception.CoordinatorException;
import com.coordinator.util.ZookeeperUtils;
import com.coordinator.worker.RecoveredAssignments.RecoveryCallback;
import com.coordinator.worker.tasks.TaskManager;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.zookeeper.CreateMode;

/**
 * @author Devy
 * @Description TODO
 * @createTime 2022年07月14日 20:04:00
 */
@Slf4j
public class WorkerLatch extends BaseLatch {

  private CuratorCache workersCache;
  private CuratorCache tasksCache;
  private TaskManager taskManager;
  private boolean runForMaster;
  private ZooKeeperChildrenEventCallback zooKeeperChildrenEventCallback;
  private TaskDistributedAlgorithm taskDistributedAlgorithm;

  public WorkerLatch(String id, String basePath, CuratorFramework client, boolean runForMaster,
      TaskDistributedAlgorithm taskDistributedAlgorithm,
      ZooKeeperChildrenEventCallback zooKeeperChildrenEventCallback
  ) throws Exception {
    this.id = id;
    this.basePath = basePath;
    this.client = client;
    ZookeeperUtils.createNodeIfNotExists(client, this.basePath + Constants.LOCK_PATH);
    this.changeLock = new InterProcessSemaphoreMutex(client, this.basePath + Constants.LOCK_PATH);
    this.runForMaster = runForMaster;
    this.zooKeeperChildrenEventCallback = zooKeeperChildrenEventCallback;
    this.taskDistributedAlgorithm=taskDistributedAlgorithm;
  }


  public void init() throws Exception {
    if (client != null && id != null && !id.isEmpty() && basePath != null && !basePath.isEmpty()
        && basePath.startsWith("/")) {
      if (!acquireLock()) {
        String msg =
            "Failed to equire lock in order to start the WorkerLatch for scope " + basePath;
        log.error(msg);
        throw new CoordinatorException(msg);
      }

      try {
        String baseWorkerPath = this.basePath + Constants.WORKERS_PATH;
        String baseAssignPath = this.basePath + Constants.ASSIGN_PATH;
        String baseTaskPath = this.basePath + Constants.TASKS_PATH;
        ZookeeperUtils.createNodeIfNotExists(client, baseWorkerPath);
        ZookeeperUtils.createNodeIfNotExists(client, baseAssignPath);
        ZookeeperUtils.createNodeIfNotExists(client, baseTaskPath);

        if (runForMaster) {
          this.leaderLatch = new LeaderLatch(this.client, this.basePath + "/master", id);
          log.info(
              "Running for master - adding listenerm starting the master cache and calling runForMaster");
          runForMaster();
          log.info("Done running the Running for master logic");

          return;
        }

        String assignPath = baseAssignPath + Constants.SLASH + id;
        if (client.checkExists().forPath(assignPath) == null) {
          log.info("Creating assign worker node for scope {}, id {} because it does not exists",
              this.basePath, this.id);
          client.create().withMode(CreateMode.PERSISTENT).forPath(assignPath);
          log.info("Created assign worker node for scope {}, id {}", this.basePath, this.id);

        }
        String workerPath = baseWorkerPath + Constants.SLASH + id;
        if (client.checkExists().forPath(workerPath) == null) {
          log.info("Creating worker node for scope {}, id {} because it does not exists",
              this.basePath, this.id);
          client.create().withMode(CreateMode.EPHEMERAL).forPath(workerPath);
          log.info("Created worker node for scope {}, id {}", this.basePath, this.id);

        } else {
          //in order to make sure that the current session with ZK is the one that "owns" the ephemeral worker's znode, we delete and recreate the node
          log.info("Recreating worker node for scope {}, id {}", this.basePath, this.id);

          CuratorOp deleteOp = client.transactionOp().delete().forPath(workerPath);
          CuratorOp createOp = client.transactionOp().create().withMode(CreateMode.EPHEMERAL)
              .forPath(workerPath);
          client.transaction().forOperations(deleteOp, createOp);
          log.info("Recreated worker node for scope {}, id {}", this.basePath, this.id);

        }
        createAndAddTasksAssignmentListener();
      } finally {
        releaseLock();
      }

    } else {
      throw new CoordinatorException("One of the passed parames is invalid.");
    }
  }


  public void createAndAddTasksAssignmentListener() throws Exception {
    log.info("Createing Tasks Assignment Listener for {}", basePath + Constants.ASSIGN_PATH);
    CuratorCache assignmentWorkers = CuratorCache
        .build(client, basePath + Constants.ASSIGN_PATH);
    assignmentWorkers.listenable().addListener(assignmentWorkerListener());
    assignmentWorkers.start();
  }


  @Override
  public void isLeader() {

    log.info("I am the leader: {}", id);
    if (!acquireLock()) {
      log.error(
          "Exception when starting leadership - Failed to equire lock in order to become leader");
    }
    try {

      this.taskManager = new TaskManager(basePath, client, taskDistributedAlgorithm);

      workersCache = CuratorCache
          .build(client, basePath + Constants.WORKERS_PATH);
      workersCache.listenable().addListener(workerListener());
      workersCache.start();

      tasksCache = CuratorCache
          .build(client, basePath + Constants.TASKS_PATH);
      tasksCache.listenable().addListener(taskListener());

      (new RecoveredAssignments(client, basePath)).recover(new RecoveryCallback() {
        @Override
        public void recoveryComplete(int rc, List<String> tasks) {
          if (rc == RecoveryCallback.FAILED) {
            log.error("Recovery of assigned tasks failed.");
          } else {
            log.debug("Assigning recovered tasks...");
            for (String task : tasks) {
              try {
                taskManager.assignTask(workersCache, task);
              } catch (Exception e) {
                log.error("Exception while executing the recovery callback for task {}", task, e);
              }
            }
            log.debug("Done assigning recovered tasks");
          }
          try {
            tasksCache.start();
          } catch (Exception e) {
            log.error("Exception when starting the tasks cache", e);
          }
        }
      });
    } catch (Exception e) {
      log.error("Exception when starting leadership", e);
    } finally {
      releaseLock();
    }
  }

  private CuratorCacheListener taskListener() {
    return CuratorCacheListener.builder().forPathChildrenCache(this.basePath + Constants.TASKS_PATH,
        client, new PathChildrenCacheListener() {
          @Override
          public void childEvent(CuratorFramework client, PathChildrenCacheEvent event)
              throws Exception {
            if (event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED) {
              try {
                taskManager
                    .assignTask(workersCache,
                        event.getData().getPath().replaceFirst(basePath + "/tasks/", ""));
              } catch (Exception e) {
                log.error("Exception when assigning task for {}", event.getData().toString(), e);
              }
            }
          }
        }).build();
  }

  @Override
  public void notLeader() {
    log.info("Lost leadership - I am {} ", id);
    try {
      if (tasksCache != null) {
        tasksCache.close();
      }
      if (workersCache != null) {
        workersCache.close();
      }
      if (globalCache != null) {
        globalCache.close();
      }
    } catch (Exception e) {
      log.info("Exception occurred during the leadership lost process", e);
    }
  }

  @Override
  public void close() {
    log.info("Closing the {} master latch and closing all of its cache nodes", basePath);
    try {
      if (tasksCache != null) {
        tasksCache.close();
      }
      if (workersCache != null) {
        workersCache.close();
      }

      if (globalCache != null) {
        globalCache.close();
      }
      if (leaderLatch != null) {
        leaderLatch.close();
      }
    } catch (Exception e) {
      log.error("Failed to close cace node", e);
    } finally {
      if (changeLock.isAcquiredInThisProcess()) {
        releaseLock();
      }
    }
  }

  private CuratorCacheListener assignmentWorkerListener() {

    return CuratorCacheListener.builder().forPathChildrenCache(
        this.basePath + Constants.ASSIGN_PATH, client, new PathChildrenCacheListener() {
          CuratorCache taskAssignment = null;

          @Override
          public void childEvent(CuratorFramework client, PathChildrenCacheEvent event)
              throws Exception {
            // The worker node itself has changed
            switch (event.getType()) {
              case CHILD_ADDED: {
                if (event.getData().getPath().endsWith(id)) {
                  if (taskAssignment != null) {
                    log.info("Assingments worker node has changed");
                    taskAssignment.close();
                  }
                  taskAssignment = CuratorCache
                      .build(client, basePath + Constants.ASSIGN_PATH_SLASH + id);

                  taskAssignment.listenable().addListener(taskAssignmentListener());
                  taskAssignment.start();
                }
                break;
              }
              default:
                break;
            }
          }
        }).build();
  }

  private CuratorCacheListener taskAssignmentListener() {

    return CuratorCacheListener.builder().forPathChildrenCache(
        basePath + Constants.ASSIGN_PATH_SLASH + id, client,
        new PathChildrenCacheListener() {
          @Override
          public void childEvent(CuratorFramework client, PathChildrenCacheEvent event)
              throws Exception {
            // a new task

            switch (event.getType()) {
              case CHILD_ADDED: {
                zooKeeperChildrenEventCallback.add(event);
                break;
              }
              case CHILD_UPDATED: {
                zooKeeperChildrenEventCallback.update(event);
                break;
              }
              case CHILD_REMOVED: {
                zooKeeperChildrenEventCallback.remove(event);
                break;
              }
              default:
                break;
            }
          }
        }).build();
  }

  private CuratorCacheListener workerListener() {
    return CuratorCacheListener.builder().
        forPathChildrenCache(this.basePath + Constants.WORKERS_PATH, client,
            new PathChildrenCacheListener() {
              @Override
              public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) {
                if (event.getType() == PathChildrenCacheEvent.Type.CHILD_REMOVED) {
                  final String workerName = event.getData().getPath()
                      .replaceFirst(basePath + Constants.WORKERS_PATH_SLASH, "");
                  log.info("A worker node {} was removed to the 'workers' node", workerName);
                  try {
                    if (!acquireLock()) {
                      log.error("Failed to acquire lock in order to recover absent worker {} tasks",
                          workerName);
                      throw new Exception();
                    }
                    RecoveredAssignments.recoverAbsentWorkerTasks(client, basePath, workerName);
                  } catch (Exception e) {
                    log.error("Exception while trying to re-assign tasks", e);
                  } finally {
                    releaseLock();
                  }
                } else if (event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED) {
                  final String workerName = event.getData().getPath()
                      .replaceFirst(basePath + Constants.WORKERS_PATH, "");
                  log.info("A new worker node {} was added to the 'workers' node", workerName);
                  try {
                    if (!acquireLock()) {
                      log.error(
                          "Failed to acquire lock in order to recreate the assign node after reconnection for {}",
                          workerName);
                      throw new Exception();
                    }
                    String assignWorkerPath = basePath + Constants.ASSIGN_PATH + workerName;
                    if (client.checkExists().forPath(assignWorkerPath) == null) {
                      log.info(
                          "The new worker node {} doesn't have a node in the 'assign' node - adding it now",
                          workerName);
                      client.create().withMode(CreateMode.PERSISTENT).forPath(assignWorkerPath);
                    }

                    (new RecoveredAssignments(client, basePath)).recover(new RecoveryCallback() {
                      @Override
                      public void recoveryComplete(int rc, List<String> tasks) {
                        if (rc == RecoveryCallback.FAILED) {
                          log.error("Recovery of assigned tasks failed.");
                        } else {
                          log.debug("Assigning recovered tasks...");
                          for (String task : tasks) {
                            try {
                              taskManager.assignTask(workersCache, task);
                            } catch (Exception e) {
                              log.error(
                                  "Exception while executing the recovery callback for task {}",
                                  task, e);
                            }
                          }
                          log.debug("Done assigning recovered tasks");
                        }
                      }
                    });
                  } catch (Exception e) {
                    log.error("Exception while trying to add a node in the 'assign' node", e);
                  } finally {
                    releaseLock();
                  }
                }
              }
            }).build();
  }

}
