package com.dhgate;

import java.util.Collection;

import com.dhgate.memcache.IMemCached;
import com.dhgate.redis.IRedisDao;
import com.dhgate.ssdb.ISSDBDao;

/**
 * 
 * @author lidingkun
 *
 */
public interface ICachedConfigManager {

	public static final String CLUSTER="cluster";
	public static final String CLUSTER_NAME="name";
	public static final String CLUSTER_FAILOVER="failover";
	
	public static final String CLUSTER_ALG="clusterAlg";
	public static final String CLUSTER_TYPE="type";
	public static final String CLUSTER_ASYNC="async";
	public static final String CLUSTER_CLIENT="client";
	public static final String CLUSTER_CLIENT_WEIGHT="weight";
	public static final String CLUSTER_CLIENT_SLAVE="slave";
	public static final String CLUSTER_CLIENT_SENTINEL="sentinel";
	public static final String CLUSTER_SENTINELMASTERNAME="sentinelMasterName";
	public static final String CLUSTER_CLIENT_PROTOCOL="protocol";
	
	public static final String POOL_TAG = "socketpool";
	public static final String POOL_TCP = "isTcp";
	public static final String POOL_INITCONN = "initConn";
	public static final String POOL_MINIDLE = "minIdle";
	public static final String POOL_MAXACTIVE = "maxActive";
	public static final String POOL_TIMEBETWEENEVICTIONRUNSMILLIS = "timeBetweenEvictionRunsMillis";
	public static final String READ_TIMEOUT = "timeout";
	public static final String POOL_TESTONBORROW = "testOnBorrow";
	public static final String POOL_MAXBUSYTIME = "maxBusyTime";
	public static final String HASHING_ALGP = "hashingAlg";
	public static final String CONNECT_TIMEOUT = "connectTimeout";
	public static final String POOL_MAXWAIT = "maxWait";
	public static final String POOL_NUMTESTSPEREVICTIONRUN = "numTestsPerEvictionRun";
	public static final String POOL_MINEVICTABLEIDLETIMEMILLIS = "minEvictableIdleTimeMillis";
	public static final String POOL_SERVERS = "servers";
	public static final String POOL_WEIGHTS = "weights";
	
	
	public static final String DIAMOND_GROUP="com.dhgate.dtb.cache";
	public static final String DIAMOND_CACHE_DEFAULT_DATAID = "dhgate_default_cache";
	
    /**
     * 获取配置在memcached.xml中的Cache客户端
     * 
     * @param name
     * @return IMemCached
     */
    public IMemCached getCache(String name);

    /**
     * 获取配置在memcached.xml中的Cache客户端Cluster
     * 
     * @param name
     * @return IMemCached (cluster)
     */
    public IMemCached getCacheCluster(String name);
    public IRedisDao getRedisCacheCluster(String name);
    public ISSDBDao getSSDBCluster(String name);
    public void destory(Collection<IMemCached> deadlist);

}
