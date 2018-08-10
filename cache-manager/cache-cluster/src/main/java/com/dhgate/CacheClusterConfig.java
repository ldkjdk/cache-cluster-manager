package com.dhgate;

import com.dhgate.CacheConfig;

/***
 * 
 * @author lidingkun
 *
 */

public class CacheClusterConfig {

	public static final String ALG_LOOGPING="looping";
	public static final String ALG_POLLING="polling";
	public static final String ALG_HASH="hash";
	public static final String REDIS_CLUSTER="redis";
	public static final String SSDB_CLUSTER="ssdb";
	
    private String   name;
    
    /**
     * The cluster access algorithms.
     * <tt>0</tt> read looping ,until got result is not null
     * 
     */
    private String clusterAlg =ALG_LOOGPING; 
    private String clusterType;
    private boolean failover=true;

    private boolean  isAsyn = true;

    
    private CacheConfig [] clients;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public CacheConfig[] getClients() {
		return clients;
	}

	public void setClients(CacheConfig[] clients) {
		this.clients = clients;
	}

	public boolean isAsyn() {
        return isAsyn;
    }

    public void setAsyn(boolean isAsyn) {
        this.isAsyn = isAsyn;
    }

	public String getClusterAlg() {
		return clusterAlg;
	}

	public void setClusterAlg(String clusterAlg) {
		this.clusterAlg = clusterAlg;
	}

	public boolean isFailover() {
		return failover;
	}

	public void setFailover(boolean failover) {
		this.failover = failover;
	}

	public String getClusterType() {
		return clusterType;
	}

	public void setClusterType(String clusterType) {
		this.clusterType = clusterType;
	}
}
