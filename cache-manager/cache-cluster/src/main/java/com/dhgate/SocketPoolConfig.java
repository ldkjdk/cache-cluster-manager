package com.dhgate;

/**
 * SocketIO Pool configuration
 * 
 * @author lidingkun
 */
public class SocketPoolConfig {

    private String  name;
    /**
     * 是否走tcp协议； true：tcp协议 false：udp协议
     */
    private boolean isTcp  = true;
    private boolean failover  = true;
    private int initConn;
    private int minIdle;
    private int maxActive;
    
    private int timeBetweenEvictionRunsMillis;
    private long minEvictableIdleTimeMillis;
    private int numTestsPerEvictionRun;
    private boolean nagle           = false;
    private int  timeout;
    private boolean testOnBorrow      = false;


    private String  servers;
    
    private String  weights;

    /**
     * Sets the max busy time for threads in the busy pool.
     */
    private long    maxBusyTime     = 1000 * 30;

   
    private int     hashingAlg      = 3;

    /**
     * socket连接超时
     */
    private int     connectTimeout ;
    private long maxWait;
    
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isFailover() {
        return failover;
    }

    public void setFailover(boolean failover) {
        this.failover = failover;
    }

    public int getInitConn() {
        return initConn;
    }

    public void setInitConn(int initConn) {
        this.initConn = initConn;
    }

    public boolean isNagle() {
        return nagle;
    }

    public void setNagle(boolean nagle) {
        this.nagle = nagle;
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

	public int getTimeBetweenEvictionRunsMillis() {
		return timeBetweenEvictionRunsMillis;
	}

	public void setTimeBetweenEvictionRunsMillis(int timeBetweenEvictionRunsMillis) {
		this.timeBetweenEvictionRunsMillis = timeBetweenEvictionRunsMillis;
	}

	public int getTimeout() {
		return timeout;
	}

	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}

	public boolean isTestOnBorrow() {
		return testOnBorrow;
	}

	public void setTestOnBorrow(boolean testOnBorrow) {
		this.testOnBorrow = testOnBorrow;
	}

	public int getConnectTimeout() {
		return connectTimeout;
	}

	public void setConnectTimeout(int connectTimeout) {
		this.connectTimeout = connectTimeout;
	}

	public String getServers() {
        return servers;
    }

    public void setServers(String servers) {
        this.servers = servers;
    }

    public String getWeights() {
        return weights;
    }

    public void setWeights(String weights) {
        this.weights = weights;
    }

   

    public boolean isTcp() {
        return isTcp;
    }

    public void setTcp(boolean isTcp) {
        this.isTcp = isTcp;
    }

    public long getMaxBusyTime() {
        return maxBusyTime;
    }

    public void setMaxBusyTime(long maxBusyTime) {
        this.maxBusyTime = maxBusyTime;
    }

    public int getHashingAlg() {
        return hashingAlg;
    }

    public void setHashingAlg(int hashingAlg) {
        this.hashingAlg = hashingAlg;
    }

    public long getMaxWait() {
		return this.maxWait;
	}

	public void setMaxWait(long maxWait) {
		this.maxWait = maxWait;
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

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + connectTimeout;
		result = prime * result + hashingAlg;
		result = prime * result + initConn;
		result = prime * result + (isTcp ? 1231 : 1237);
		result = prime * result + ((servers == null) ? 0 : servers.hashCode());
		result = prime * result + timeout;
		result = prime * result + ((weights == null) ? 0 : weights.hashCode());
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
		SocketPoolConfig other = (SocketPoolConfig) obj;
		if (connectTimeout != other.connectTimeout)
			return false;
		if (hashingAlg != other.hashingAlg)
			return false;
		if (initConn != other.initConn)
			return false;
		if (isTcp != other.isTcp)
			return false;
		if (servers == null) {
			if (other.servers != null)
				return false;
		} else if (!servers.equals(other.servers))
			return false;
		if (timeout != other.timeout)
			return false;
		if (weights == null) {
			if (other.weights != null)
				return false;
		} else if (!weights.equals(other.weights))
			return false;
		return true;
	}
	
	
	
	
}
