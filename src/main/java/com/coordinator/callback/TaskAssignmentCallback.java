package com.coordinator.callback;

import com.coordinator.worker.tasks.Task;

public interface TaskAssignmentCallback {
	

	public void start( Task task ) ;

	public void stop( Task task ) ;


}
