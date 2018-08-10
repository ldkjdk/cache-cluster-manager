package com.dhgate.ssdb;

import org.apache.commons.pool.BasePoolableObjectFactory;

import com.dhgate.ssdb.client.SSDB;

/**
 * The pool factory class which is used to make/activate/passivate/validate/destroy pool object.
 * The activate/passivate methods are empty.
 * 
 *@author lidingkun
 *
 */
class SSDBPoolFactory extends BasePoolableObjectFactory<SSDB> {
	private String ip;
	private int port;
	private int timeout;

	public SSDBPoolFactory(String ip, int port, int timeout) {
		this.ip = ip;
		this.port = port;
		this.timeout = timeout;
	}

	@Override
	public SSDB makeObject() throws Exception {
		final SSDB ssdb = new SSDB(ip, port, timeout);
		return ssdb;
	}

	@Override
	public void activateObject(SSDB ssdb) throws Exception {

	}

	@Override
	public void destroyObject(SSDB ssdb) throws Exception {
		//There is no quit command in ssdb.
//		try{
//			ssdb.quit();
//		}catch(Exception e){
//			e.printStackTrace();
//		}
		try{
			ssdb.close();
		}catch(Exception e){
			e.printStackTrace();
		}
	}

	@Override
	public void passivateObject(SSDB ssdb) throws Exception {

	}

	@Override
	public boolean validateObject(SSDB ssdb) {
		try {
			return ssdb.ping();
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}
}
