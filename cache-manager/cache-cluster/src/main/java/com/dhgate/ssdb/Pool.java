package com.dhgate.ssdb;

import java.io.Closeable;

import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;

/**
 * The abstract pool to provide the basic methods, including getResource, returnResource, returnBrokenResource
 *  and destroy.
 * 
 *
 * @author lidingkun
 *
 */
public abstract class Pool<T> implements Closeable {
	protected GenericObjectPool<T> internalPool = null;

	public Pool(final GenericObjectPool.Config poolConfig,
			PoolableObjectFactory<T> factory) {
		initPool(poolConfig, factory);
	}

	public void initPool(final GenericObjectPool.Config poolConfig,
			PoolableObjectFactory<T> factory) {
		if (this.internalPool != null) {
			try {
				destroy();
			} catch (Exception e) {

			}
		}
		this.internalPool = new GenericObjectPool<T>(factory, poolConfig);
	}

	public T getResource() {
		try {
			return internalPool.borrowObject();
		} catch (Exception e) {
			throw new SSDBException("Could not get a resource from the pool", e);
		}
	}

	protected void returnResource(final T resource) {
		if (resource == null) {
			return;
		}
		try {
			internalPool.returnObject(resource);
		} catch (Exception e) {
			throw new SSDBException(
					"Could not return the resource to the pool", e);
		}
	}

	protected void returnBrokenResource(final T resource) {
		try {
			internalPool.invalidateObject(resource);
		} catch (Exception e) {
			throw new SSDBException(
					"Could not return the broken resource to the pool", e);
		}
	}

	public void destroy() {
		try {
			internalPool.close();
		} catch (Exception e) {
			throw new SSDBException("Could not destroy the pool", e);
		}
	}


	@Override
	public void close() {
		destroy();
	}
	
	
	public void setConfig(GenericObjectPool.Config conf) {
		this.internalPool.setConfig(conf);
	}
}
