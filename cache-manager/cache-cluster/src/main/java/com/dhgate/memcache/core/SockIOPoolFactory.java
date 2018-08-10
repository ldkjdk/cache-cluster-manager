package com.dhgate.memcache.core;

import org.apache.commons.pool.BasePoolableObjectFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dhgate.memcache.schooner.SchoonerSockIO;
import com.dhgate.memcache.schooner.SchoonerSockIOPool;


/**
 * The pool factory class which is used to make/activate/passivate/validate/destroy pool object.
 * The activate/passivate methods are empty.
 * 
 * @since 1.2.0
 * @author lidingkun 
 *
 */

public class SockIOPoolFactory extends BasePoolableObjectFactory<SchoonerSockIO>{
	private static Logger log = LoggerFactory.getLogger(SockIOPoolFactory.class);

	private boolean isTcp=true;
	private String host;
	private int bufferSize = 1024 * 1025;
	private int socketTO = 1000 * 30; // default timeout of socket reads
	private int socketConnectTO = 1000 * 3; // default timeout of socket
	private boolean nagle = false; // enable/disable Nagle's algorithm
	
	
	public SockIOPoolFactory(boolean isTcp, String host,
			int socketTO, int socketConnectTO) {
		super();
		this.isTcp = isTcp;
		this.host = host;
		this.socketTO = socketTO;
		this.socketConnectTO = socketConnectTO;
	}
	
	
	public SockIOPoolFactory(boolean isTcp, String host, int bufferSize,
			int socketTO, int socketConnectTO, boolean nagle) {
		super();
		this.isTcp = isTcp;
		this.host = host;
		this.bufferSize = bufferSize;
		this.socketTO = socketTO;
		this.socketConnectTO = socketConnectTO;
		this.nagle = nagle;
	}

	@Override
	public SchoonerSockIO makeObject() throws Exception {
		SchoonerSockIO socket = null;
		try {
			if (isTcp) {
				socket = new TCPSockIO(host, bufferSize, this.socketTO, this.socketConnectTO, this.nagle, false);
			} else {
				socket = new UDPSockIO(host, bufferSize, socketTO, false);
			}
		} catch (Exception ex) {
			log.error("makeObject for " + this.host, ex);
			socket = null;
		}

		return socket;
	}

	@Override
	public void destroyObject(SchoonerSockIO obj) throws Exception {
		obj.trueClose();
	}

	@Override
	public boolean validateObject(SchoonerSockIO obj) {
		// TODO Auto-generated method stub
		return obj.isAlive();
	}

	@Override
	public void activateObject(SchoonerSockIO obj) throws Exception {
		// TODO Auto-generated method stub
		//super.activateObject(obj);
	}

	@Override
	public void passivateObject(SchoonerSockIO obj) throws Exception {
		// TODO Auto-generated method stub
		//super.passivateObject(obj);
	}
	
	

}
