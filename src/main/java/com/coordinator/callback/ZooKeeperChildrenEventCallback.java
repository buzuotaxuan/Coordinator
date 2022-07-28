package com.coordinator.callback;

import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;


public interface ZooKeeperChildrenEventCallback {


	public void add( PathChildrenCacheEvent event ) throws Exception;

	public void remove( PathChildrenCacheEvent event ) throws Exception;

	public void update( PathChildrenCacheEvent event ) throws Exception;

}
