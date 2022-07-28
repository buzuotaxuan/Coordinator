package com.coordinator.worker;

import java.util.List;

/**
 * 
 */
public class DistributionTask {
	
	public String basePath;
	public String taskPath;
	public List<String> workers;
	
	public DistributionTask( String basePath, String taskPath, List<String> workers ) {
		this.basePath = basePath;
		this.taskPath = taskPath;
		this.workers = workers;
	}

}
