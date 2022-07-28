package com.coordinator.util;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException.NodeExistsException;

/**
 * @author Devy
 * @Description TODO
 * @createTime 2022年07月22日 14:06:00
 */
@Slf4j
@Data
public class ZookeeperUtils {


  public static void createNodeIfNotExists(CuratorFramework client,String path) {
    try {
      client.create().creatingParentsIfNeeded().forPath(path);
    } catch (NodeExistsException e) {
      log.info("node exists:" + path);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}
