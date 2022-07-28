package com.coordinator.worker.tasks;

import java.util.Map;
import lombok.Data;

/**
 * @author Devy
 * @Description TODO
 * @createTime 2022年07月14日 20:17:00
 */
@Data
public class Task {

  private String id;
  Map metaMap;
  private String path;

}
