package com.coordinator.service;

import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.CuratorFrameworkFactory.Builder;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * @author Devy
 * @Description TODO
 * @createTime 2022年07月14日 17:08:00
 */
@Slf4j
public class ZookeeperService {

  public static CuratorFramework init( List<String> zkServersIps, String namespace, String username, String password) {
    log.info( "Creating CuratorFramework client" );
    if ( namespace != null && !namespace.isEmpty() && namespace.equalsIgnoreCase( "zookeeper" ) ) {
      throw new IllegalArgumentException( "Namespace can't be 'zookeeper' " );
    }
    Builder curatorFrameworkBuilder = CuratorFrameworkFactory.builder();
    curatorFrameworkBuilder.connectString( getZKServersConnectionString( zkServersIps) );
    curatorFrameworkBuilder.retryPolicy( new ExponentialBackoffRetry( 1000, 5 ) );

    if ( namespace != null && !namespace.isEmpty() ) {
      curatorFrameworkBuilder.namespace( namespace );
    }

    CuratorFramework client = curatorFrameworkBuilder.build();
    client.getUnhandledErrorListenable().addListener( errorsListener );

    log.info( "Starting the CuratorFramework client" );
    int maxBlockTime = Integer.parseInt( System.getProperty( "zookeeper.curator.block.until.connected.max.time.in.seconds", "5" ) );
    client.start();

    boolean connected = false;
    try {
      log.info( "Checking for connection to zookeeper..." );
      connected = client.blockUntilConnected( maxBlockTime, TimeUnit.SECONDS );
    }
    catch ( InterruptedException e ) {
      log.error( "CuratorFramework client failed to connect to zookeeper - Max time for trying is {} seconds", maxBlockTime, e );
    }
    if ( !connected ) {
      log.error( "Failed to connect to zookeeper - Max time for trying is {} seconds", maxBlockTime );
      closeClient( client );
      return null;
    }
    log.info( "CuratorFramework started" );
    return client;
  }

  private static String getZKServersConnectionString( List<String> zkServersIps) {
    log.info( "Zookeeper servers list is - {}", zkServersIps );
    StringBuilder sb = new StringBuilder();
    String delim = "";
    for ( String hostname : zkServersIps ) {
      sb.append( delim );
      sb.append( hostname );
      delim = ",";
    }
    return sb.toString();
  }

  private static UnhandledErrorListener errorsListener = new UnhandledErrorListener() {
    @Override
    public void unhandledError( String message, Throwable e ) {
      log.error( "Curator Client Error: {}", message, e );
    }
  };

  public static void closeClient( CuratorFramework client ) {
    log.info( "Closing the CuratorFramework client" );
    if ( client != null ) {
      client.close();
      try {
        log.info( "Taking a sleep in order to make sure all connections are close" );
        Thread.sleep( Integer.parseInt( System.getProperty( "zookeeper.close.client.sleep.timeinmillis", "10000" ) ) );
        log.info( "Closed the CuratorFramework client" );
      }
      catch ( InterruptedException e ) {
        log.error( "Failed to take a 5 seconds sleep in order to make sure all connections are close", e );
      }
    }
    else {
      log.error( "Failed to close zookeeper client - passed client is null" );
    }
  }

}
