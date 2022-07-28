package com.coordinator.worker.algo;

import com.coordinator.callback.TaskDistributedAlgorithm;
import com.coordinator.worker.DistributionTask;
import java.util.Random;


public class RandomTaskDistributionAlgorithm implements TaskDistributedAlgorithm {

	private Random rand = new Random( System.currentTimeMillis() );

	@Override
	public String getTaskTargetWorker( DistributionTask task ) {
		int index = rand.nextInt( task.workers.size() );
		String designatedWorker = task.workers.get( index ).replaceFirst( task.basePath + "/workers/", "" );
		return designatedWorker;
	}

	@Override
	public void init() {}

}
