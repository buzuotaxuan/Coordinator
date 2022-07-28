package com.coordinator.worker;

import com.coordinator.constants.Constants;
import java.util.ArrayList;
import java.util.List;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slf4j
public class RecoveredAssignments {

	private RecoveryCallback cb;
	private CuratorFramework client;
	private String basePath;

	public interface RecoveryCallback {
		final static int OK = 0;
		final static int FAILED = -1;

		public void recoveryComplete( int rc, List<String> tasks );
	}


	public RecoveredAssignments( CuratorFramework client, String path ) {
		this.client = client;
		this.basePath = path;
	}


	public void recover( RecoveryCallback recoveryCallback ) throws Exception {
		log.info( "Starting to recover tasks..." );
		cb = recoveryCallback;
		// Current tasks
		List<String> tasks = client.getChildren().forPath( basePath + "/tasks" );
		// current assigned workers
		List<String> assignedWorkers = client.getChildren().forPath( basePath + "/assign" );
		// current live workers
		List<String> workers = client.getChildren().forPath( basePath + "/workers" );
		// if there are no workers - really an edge case - should not happen
		if ( workers.size() == 0 ) {
			log.warn( "Empty list of workers, possibly just starting" );
			cb.recoveryComplete( RecoveryCallback.OK, new ArrayList<String>() );
			return;
		}
		else {
			// check all the workers under the /assign znode
			for ( String assignedWorker : assignedWorkers ) {
				// if the tasks belongs to a worker that no linger exists - it should be re-assigned
				if ( !workers.contains( assignedWorker ) ) {
					recoverAbsentWorkerTasks( client, basePath, assignedWorker, tasks, true );
				}
			}
			log.info( "finished the recover tasks call - total tasks to recover is {}", tasks.size() );
			cb.recoveryComplete( RecoveryCallback.OK, tasks );
		}
	}
	

	public static void recoverAbsentWorkerTasks( CuratorFramework client, String basePath, String assignedWorkerName ) {
		recoverAbsentWorkerTasks( client, basePath, assignedWorkerName, null, false );
	}
	

	private static void recoverAbsentWorkerTasks( CuratorFramework client, String basePath, String assignedWorkerName, List<String> tasksList, boolean updateTasksList ) {
		try {
			// get each assignment znode tasks
			List<String> assigendTasks = client.getChildren().forPath( basePath + "/assign"+Constants.SLASH + assignedWorkerName );
			log.info( "Recovering {} tasks from assigned worker {}", assigendTasks.size(), assignedWorkerName );
			for ( String task : assigendTasks ) {
				handleMissingWorkerTask( client, basePath, task, assignedWorkerName, tasksList, updateTasksList );
			}
			// once all the tasks has been reassigned - deleted the assignment znode
			log.info( "Deleting node /assign/{}", assigendTasks );
			client.delete().guaranteed().forPath( basePath + "/assign"+Constants.SLASH + assignedWorkerName );
			log.info( "Deleted node /assign/{}", assigendTasks );
		}
		catch ( Exception e ) {
			log.error( "Failed to recover tasks from assigned worker {}", assignedWorkerName, e );
		}
	}
	
	private static void handleMissingWorkerTask( CuratorFramework client, String basePath, String task, String assignedWorkerName, List<String> tasksList, boolean updateTasksList ) {
		try {
			// recreate each task under the /tasks znode and delete if from the assignment znode
			String taskPath=basePath+ Constants.TASKS_PATH_SLASH+task;
			String assignPath=basePath+Constants.ASSIGN_PATH_SLASH+assignedWorkerName+Constants.SLASH+task;
			CuratorOp createOp = client.transactionOp().create().withMode(CreateMode.PERSISTENT).forPath(taskPath,client.getData().forPath(assignPath));
			CuratorOp deleteOp = client.transactionOp().delete().forPath(assignPath);
			client.transaction().forOperations(deleteOp, createOp);
			log.info( "Moved task {} from /assign/{} to tasks/{}", task, assignedWorkerName, task );
			if ( updateTasksList ) {
				// add the tasks to the tasks list
				tasksList.add( task );
			}
		}
		catch ( Exception e ) {
			log.error( "Failed to complete transaction for assigning task {} to the tasks node and the delete it from worker {}", task, assignedWorkerName, e );
		}
	}
}
