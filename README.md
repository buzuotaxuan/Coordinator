**分布式任务协调器**

结构图

![img.png](img.png)

节点角色说明
master : 负责任务分配
worker : 负责接受任务执行
tasks  : 任务新建后会在这个目录下面，然后会被master 根据算法分配到woker 节点执行
assign : 保存正在执行的woker 和任务的关系

使用
参见 CoordinatorManagerTest 类