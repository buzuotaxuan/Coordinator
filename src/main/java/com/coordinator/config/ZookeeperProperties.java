package com.coordinator.config;

import java.util.List;
import lombok.Data;

/**
 * @author Devy
 * @Description TODO
 * @createTime 2022年07月14日 17:11:00
 */
@Data
public class ZookeeperProperties {
  List<String> zkServersIps;
  String namespace;
  String username;
  String password;
}
