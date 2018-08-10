package com.dhgate.ssdb;

import org.apache.commons.pool.impl.GenericObjectPool.Config;

import com.dhgate.ssdb.client.SSDB;

/**
 * The SSDBPool use SSDBPoolFactory to create and manage the resource object in the pool.
 * 
 *
 * @author lidingkun
 *
 */
public class SSDBPool extends Pool<SSDB> {
	private String host;
	private int port;
	private int timeout;
	
	public SSDBPool(Config config, String ip, int port, int timeout) {
		super(config, new SSDBPoolFactory(ip, port, timeout));
		this.host = ip;
		this.port = port;
		this.timeout = timeout;
	}

	@Override
	public SSDB getResource() {
		SSDB ssdb = super.getResource();
		ssdb.setPool(this);
		return ssdb;
	}

	public String getHost(){
		return host;
	}
	
	public int getPort(){
		return port;
	}
	
	public int getTimeout(){
		return timeout;
	}

	@Override
	public String toString() {
		return "SSDBPool [host=" + host + ", port=" + port + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((host == null) ? 0 : host.hashCode());
		result = prime * result + port;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SSDBPool other = (SSDBPool) obj;
		if (host == null) {
			if (other.host != null)
				return false;
		} else if (!host.equals(other.host))
			return false;
		if (port != other.port)
			return false;
		return true;
	}
	
	
}
