package com.coordinator.worker;

import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCache.Options;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;

/**
 * @author Devy
 * @Description TODO
 * @createTime 2022年07月14日 20:02:00
 */
@Slf4j
public abstract class BaseLatch implements LeaderLatchListener {

  public String id;

  public String basePath;

  public CuratorFramework client;

  public LeaderLatch leaderLatch;

  public InterProcessSemaphoreMutex changeLock;


  public int nodeChangeLockAcquireTime = 60;
  public int nodeChangeLockSleepTime = 1;
  public int maxNumberOfTriesForLock = 5;

  public CuratorCache globalCache;


  public void runForMaster(){
    globalCache =CuratorCache.build(client,basePath);
    globalCache.listenable().addListener(globalCacheListener());
    globalCache.start();
    leaderLatch.addListener( this );
    try {
      leaderLatch.start();
    } catch (Exception exception) {
      exception.printStackTrace();
    }
  }

  private CuratorCacheListener globalCacheListener() {
    return CuratorCacheListener.builder()
        .forTreeCache(client,new TreeCacheListener() {
          @Override
          public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
            String path = null;
            ChildData childData = event.getData();
            if ( childData != null ) {
              path = childData.getPath();
            }
            switch ( event.getType() ) {
              case CONNECTION_LOST:
                log.debug( "event: CONNECTION_LOST" );
                break;
              case CONNECTION_RECONNECTED:
                log.debug( "event: CONNECTION_RECONNECTED" );
                break;
              case CONNECTION_SUSPENDED:
                log.debug( "event: CONNECTION_SUSPENDED" );
                break;
              case INITIALIZED:
                log.debug( "scope cache INITIALIZED for scope  {}", basePath );
                break;
              case NODE_ADDED:
                if ( path != null && path.contains( "/assign" ) ) {
                  log.debug( "Task assigned correctly for path: {}", path );
                }
                else if ( path != null && path.contains( "/tasks" ) ) {
                  log.debug( "new Task added for path: {}", path );
                }
                else if ( path != null && path.contains( "/workers" ) ) {
                  log.debug( "worker added for path: {}", path );
                }
                else if ( path != null && path.contains( "/master" ) ) {
                  log.debug( "master added for path: {}", path );
                }
                else {
                  log.debug( "NODE_ADDED for path {}", path );
                }
                break;
              case NODE_REMOVED:
                if ( path != null && path.contains( "/tasks" ) ) {
                  log.debug( "Task correctly deleted from the /tasks/ node: {}", path );
                }
                else if ( path != null && path.contains( "/assign" ) ) {
                  log.debug( "Task correctly deleted from the /assign/ node: {}", path );
                  break;
                }
                else if ( path != null && path.contains( "/workers" ) ) {
                  log.debug( "worker removed for path: {}", path );
                }
                else if ( path != null && path.contains( "/master" ) ) {
                  log.debug( "master removed for path: {}", path );
                }
                else {
                  log.debug( "NODE_REMOVED for path {}", path );
                }
                break;
              case NODE_UPDATED:
                log.debug( "NODE_UPDATED for path {}", path );
                break;
              default:
                break;
            }
          }
        }).build();
  }

  public boolean hasLeadership() {
    return leaderLatch != null ? leaderLatch.hasLeadership() : false;
  }

  public abstract void close();


  public boolean acquireLock() {
    int tries = 0;
    boolean result = false;
    log.info( "Trying to aquired the nodesChangeLock for scope {}", basePath );
    // we try to acquire the lock for a max number of tries
    while ( tries < maxNumberOfTriesForLock ) {
      boolean locked = false;
      boolean hasException = false;
      try {
        locked = changeLock.acquire( nodeChangeLockAcquireTime, TimeUnit.SECONDS );
        if ( locked ) {
          log.info( "Successfully aquired the nodesChangeLock for scope {}", basePath );
          result = true;
          break;
        }
      }
      catch ( Exception e ) {
        // This can happen in cases of ZK errors or connection interruptions
        hasException = true;
      }
      if ( hasException ) {
        log.error( "An error related to ZK occurd while trying to fetch the nodesChangeLock for scope {}. Sleeping for {} seconds - try number {} out of {}", basePath, nodeChangeLockSleepTime, tries, maxNumberOfTriesForLock );
      }
      else {
        //failed to acquire the lock in the given time frame
        log.error( "Failed to fetch the nodesChangeLock for scope {} in the given timeframe of {} seconds. Sleeping for {} seconds - try number {} out of {}", basePath, nodeChangeLockAcquireTime, nodeChangeLockSleepTime, tries, maxNumberOfTriesForLock );
      }
      try {
        Thread.sleep( nodeChangeLockSleepTime * 1000 );
        log.error( "Slept for {} seconds for scope {}", nodeChangeLockSleepTime, basePath );
      }
      catch ( InterruptedException e ) {
        log.error( "Failed to sleep for {} seconds", nodeChangeLockSleepTime, e );
      }
      tries++;
    }
    if ( !result ) {
      log.error( "Failed to fetch the nodesChangeLock for scope {} after {} tries", basePath, tries );
    }
    return result;
  }

  public void releaseLock() {
    log.info( "Releasing the nodesChangeLock for scope {}", basePath );
    try {
      changeLock.release();
    }
    catch ( Exception e ) {
      log.error( "Exception when trying to release the nodesChangeLock for scope {}", basePath, e );
    }
  }


}
