package com.dhgate;
/**
 * 
 * 
 * @author lidingkun
 *
 */
public class CacheConfig {

	public static final String REDIS = "redis";
	public static final String SSDB = "ssdb";
	public static final String MEMCACHED = "memc";
    private String  name;

    /**
     *  true：tcp, false：udp
     */
    private boolean isTcp    = true;

    private String protocol =MEMCACHED;
    /**
     *  true：binary, false：ascii text
     */
    private boolean isBinary = false;
    
    private boolean compressEnable;
    
    private String  defaultEncoding="UTF-8";
   
    private SocketPoolConfig  socketPool;
    
    /**
     * for redis client
     */
    private boolean isSlave = false;
    private boolean isSentinel = false; 
    private String sentinelName;
    private int weight =1;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isCompressEnable() {
        return compressEnable;
    }

    public void setCompressEnable(boolean compressEnable) {
        this.compressEnable = compressEnable;
    }

    public String getDefaultEncoding() {
        return defaultEncoding;
    }

    public void setDefaultEncoding(String defaultEncoding) {
        this.defaultEncoding = defaultEncoding;
    }

    public SocketPoolConfig getSocketPool() {
		return socketPool;
	}

	public void setSocketPool(SocketPoolConfig socketPool) {
		this.socketPool = socketPool;
	}

	public boolean isTcp() {
        return isTcp;
    }

    public void setTcp(boolean isTcp) {
        this.isTcp = isTcp;
    }

    public boolean isBinary() {
        return isBinary;
    }

    public void setBinary(boolean isBinary) {
        this.isBinary = isBinary;
    }

	public String getProtocol() {
		return protocol;
	}

	public void setProtocol(String protocol) {
		this.protocol = protocol;
	}

	public boolean isSlave() {
		return isSlave;
	}

	public void setSlave(boolean isSlave) {
		this.isSlave = isSlave;
	}

	public boolean isSentinel() {
		return isSentinel;
	}

	public void setSentinel(boolean isSentinel) {
		this.isSentinel = isSentinel;
	}

	public int getWeight() {
		return weight;
	}

	public void setWeight(int weight) {
		this.weight = weight;
	}

	public String getSentinelName() {
		return sentinelName;
	}

	public void setSentinelName(String sentinelName) {
		this.sentinelName = sentinelName;
	}
}
