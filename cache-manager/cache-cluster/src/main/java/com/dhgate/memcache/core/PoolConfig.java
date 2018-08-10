package com.dhgate.memcache.core;



import org.apache.commons.pool.impl.GenericObjectPool.Config;

/**
 * The config class for the pool initialization. The usage follows the guide in common-pools-1.6.
 * 
 * @since 1.2.0
 * @author lidingkun
 *
 */
public class PoolConfig extends Config {


	private boolean isTcp=true;
	private String host;
	//private int bufferSize = 1024 * 1025;
	private int timeout = 2000; // default timeout of socket reads
	private int connectTimeout = 30000; // default timeout of socket
	private boolean nagle = false; // enable/disable Nagle's algorithm
	private int initialSize = 5;
	
	public PoolConfig() {
		setTestWhileIdle(true);
		setNumTestsPerEvictionRun(-1);
		setMaxIdle(100);
		setMaxActive(100);
		setInitialSize(10);
		setMaxWait(5000);
		setTimeBetweenEvictionRunsMillis(60000);
		setMinEvictableIdleTimeMillis(60000*5);
		//setTimeBetweenEvictionRunsMillis();
	}

	public int getMaxIdle() {
		return maxIdle;
	}

	public void setMaxIdle(int maxIdle) {
		this.maxIdle = maxIdle;
	}

	public int getMinIdle() {
		return minIdle;
	}

	public void setMinIdle(int minIdle) {
		this.minIdle = minIdle;
	}

	public int getMaxActive() {
		return maxActive;
	}

	public void setMaxActive(int maxActive) {
		this.maxActive = maxActive;
	}

	public long getMaxWait() {
		return maxWait;
	}

	public void setMaxWait(long maxWait) {
		this.maxWait = maxWait;
	}

	public byte getWhenExhaustedAction() {
		return whenExhaustedAction;
	}

	public void setWhenExhaustedAction(byte whenExhaustedAction) {
		this.whenExhaustedAction = whenExhaustedAction;
	}

	public boolean isTestOnBorrow() {
		return testOnBorrow;
	}

	public void setTestOnBorrow(boolean testOnBorrow) {
		this.testOnBorrow = testOnBorrow;
	}

	public boolean isTestOnReturn() {
		return testOnReturn;
	}

	public void setTestOnReturn(boolean testOnReturn) {
		this.testOnReturn = testOnReturn;
	}

	public boolean isTestWhileIdle() {
		return testWhileIdle;
	}

	public void setTestWhileIdle(boolean testWhileIdle) {
		this.testWhileIdle = testWhileIdle;
	}

	public long getTimeBetweenEvictionRunsMillis() {
		return timeBetweenEvictionRunsMillis;
	}

	public void setTimeBetweenEvictionRunsMillis(
			long timeBetweenEvictionRunsMillis) {
		this.timeBetweenEvictionRunsMillis = timeBetweenEvictionRunsMillis;
	}

	public int getNumTestsPerEvictionRun() {
		return numTestsPerEvictionRun;
	}

	public void setNumTestsPerEvictionRun(int numTestsPerEvictionRun) {
		this.numTestsPerEvictionRun = numTestsPerEvictionRun;
	}

	public long getMinEvictableIdleTimeMillis() {
		return minEvictableIdleTimeMillis;
	}

	public void setMinEvictableIdleTimeMillis(long minEvictableIdleTimeMillis) {
		this.minEvictableIdleTimeMillis = minEvictableIdleTimeMillis;
	}

	public long getSoftMinEvictableIdleTimeMillis() {
		return softMinEvictableIdleTimeMillis;
	}

	public void setSoftMinEvictableIdleTimeMillis(
			long softMinEvictableIdleTimeMillis) {
		this.softMinEvictableIdleTimeMillis = softMinEvictableIdleTimeMillis;
	}

	public boolean isTcp() {
		return isTcp;
	}

	public void setTcp(boolean isTcp) {
		this.isTcp = isTcp;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getTimeout() {
		return timeout;
	}

	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}

	public int getConnectTimeout() {
		return connectTimeout;
	}

	public void setConnectTimeout(int connectTimeout) {
		this.connectTimeout = connectTimeout;
	}

	public boolean isNagle() {
		return nagle;
	}

	public void setNagle(boolean nagle) {
		this.nagle = nagle;
	}

	public int getInitialSize() {
		return initialSize;
	}

	public void setInitialSize(int initialSize) {
		this.initialSize = initialSize;
	}

	
	public boolean isDifferent(PoolConfig o) {
		if (o == null) return true;
		//(this.initialSize != o.getInitialSize()) ||
		 if ( (this.minIdle != o.getMinIdle()) 
				 || (this.timeBetweenEvictionRunsMillis != o.getTimeBetweenEvictionRunsMillis())
				 || (this.minEvictableIdleTimeMillis != o.getMinEvictableIdleTimeMillis())
				 || (this.numTestsPerEvictionRun != o.getNumTestsPerEvictionRun())
				 || ( this.maxIdle != o.getMaxActive()) 
				 || (this.maxActive != o.getMaxActive()) 
				 || ( this.maxWait != o.getMaxWait()) 
				 || (this.testOnBorrow != o.isTestOnBorrow())) 
			      return true;
        
       return false;
	}
}
