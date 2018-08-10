package com.dhgate.memcache.core;

import java.io.Closeable;
import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;
/**
 * 
 * @author lidingkun
 *
 * @param <T>
 */

public abstract class Pool<T> implements Closeable {
	protected GenericObjectPool<T> internalPool = null;
	protected PoolConfig config;
	
	public Pool(final PoolConfig poolConfig,
			PoolableObjectFactory<T> factory) throws Exception {
		this.config = poolConfig;
		initPool(poolConfig, factory);
	}

	public void initPool(final GenericObjectPool.Config poolConfig,
			PoolableObjectFactory<T> factory) throws Exception {
		if (this.internalPool != null) {
			try {
				destroy();
			} catch (Exception e) {

			}
		}
		this.internalPool = new GenericObjectPool<T>(factory, poolConfig);
		
		 try {
	            for (int i = 0 ; i < config.getInitialSize() ; i++) {
	            	internalPool.addObject();
	            }
	        } catch (Exception e) {
	            throw new Exception ("Error preloading the connection pool", e);
	        }
		
	}

	public T getResource() throws Exception {
		
		return internalPool.borrowObject();
		
	}

	protected void returnResource(final T resource) throws Exception {
		if (resource == null) {
			return;
		}
		
		internalPool.returnObject(resource);
		
	}

	protected void returnBrokenResource(final T resource) throws Exception {
		
		internalPool.invalidateObject(resource);
		
	}

	public void destroy() throws Exception {
		
		internalPool.close();
		
	}

	@Override
	public void close() {
		try {
			destroy();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public int getInitialSize() {
		return config.getInitialSize();
	}

	public void setInitialSize(int initialSize) {
		this.config.setInitialSize(initialSize);
	}

	public PoolConfig getConfig() {
		return config;
	}

	public void setConfig(PoolConfig config) {
		this.config = config;
		internalPool.setConfig(config);
	}
}
