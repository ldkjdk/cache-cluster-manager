package com.dhgate.memcache.core;

import com.dhgate.memcache.schooner.SchoonerSockIO;
/**
 * 
 * @author lidingkun
 *
 */
public class SocketPool extends Pool <SchoonerSockIO>{
	private String host=null;
	

	public SocketPool (PoolConfig  config,  boolean isTcp,String host,int timeout,int connectTimeout) throws Exception {
		super(config, new SockIOPoolFactory(isTcp,host,timeout,connectTimeout));
		this.host=host;
		
	}

	@Override
	public SchoonerSockIO getResource() throws Exception {
		SchoonerSockIO ob = super.getResource();
		ob.setPool(this);
		return ob;
	}

	@Override
	public void returnResource(SchoonerSockIO resource) throws Exception {
		// TODO Auto-generated method stub
		super.returnResource(resource);
	}

	@Override
	public void returnBrokenResource(SchoonerSockIO resource)
			throws Exception {
		// TODO Auto-generated method stub
		super.returnBrokenResource(resource);
	}

	public String getHost() {
		return host;
	}
}
