package com.coordinator.callback;


import com.coordinator.worker.DistributionTask;

public interface TaskDistributedAlgorithm {


	public String getTaskTargetWorker(DistributionTask task) throws Exception;

	public void init();
}
