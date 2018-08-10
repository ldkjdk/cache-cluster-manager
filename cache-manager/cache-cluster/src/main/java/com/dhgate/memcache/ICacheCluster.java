package com.dhgate.memcache;

import java.util.List;

import com.dhgate.ICachedConfigManager;

public interface ICacheCluster {
	public static final int PING_CN =2;
	void init();
	List<IMemCached> copyCaches();
	void setCaches(List<IMemCached> caches);

	void setCacheManager(ICachedConfigManager cacheManager);

	String getClusterAlg();
	void setClusterAlg(String clusterAlg);
	boolean isFailover();
    void setFailover(boolean failover) ;
    public void setName(String name);
    public String getName();
    public void setAsyn(boolean isAsyn);
}
