package com.dhgate.cache.config.diamond;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dhgate.CacheConfigurationPaser;
import com.dhgate.ICachedConfigManager;
import com.dhgate.memcache.CacheClusterConfig;
import com.dhgate.memcache.CacheConfig;
import com.dhgate.memcache.ICacheCluster;
import com.dhgate.memcache.IMemCached;
import com.dhgate.memcache.MemcachedCache;
import com.dhgate.memcache.MemcachedClientCluster;
import com.dhgate.memcache.SocketPoolConfig;
import com.dhgate.memcache.core.MemCachedClient;
import com.dhgate.memcache.core.PoolConfig;
import com.dhgate.memcache.schooner.SchoonerSockIOPool;
import com.dhgate.redis.AbstractRedisDao;
import com.dhgate.redis.IRedisDao;
import com.dhgate.redis.RedisCache;
import com.dhgate.redis.RedisCluster;
import com.dhgate.redis.SentinelRedisCache;
import com.dhgate.redis.clients.jedis.JedisPoolConfig;
import com.dhgate.ssdb.AbstractSSDBDao;
import com.dhgate.ssdb.ISSDBDao;
import com.dhgate.ssdb.SSDBCache;
import com.dhgate.ssdb.SSDBCluster;
import com.taobao.diamond.manager.ManagerListener;
import com.taobao.diamond.manager.impl.DefaultDiamondManager;

/**
 * 
 * 
 * @author  lidingkun
 */
public class DiamondConfiguration implements ICachedConfigManager {

    private volatile static ICachedConfigManager cacheManager = null;

    private static final Logger log          = LoggerFactory.getLogger (DiamondConfiguration .class);
    private ScheduledExecutorService failoverThread = Executors.newScheduledThreadPool(1);
    private String dataId;
    private String group;
    private DiamondConfiguration(String dataId,String group){
    	this.dataId = dataId.trim();
    	this.group = group.trim();
        init();
    }

    public static ICachedConfigManager getInstance(String dataId,String group) {
        if (cacheManager == null) {
            synchronized (DiamondConfiguration.class) {
                if (cacheManager == null) cacheManager = new DiamondConfiguration(dataId,group);
            }
        }
        return cacheManager;
    }

    
    
    private ConcurrentHashMap<String, IMemCached>           cachepool;
   
    private ConcurrentHashMap<String, IMemCached>           clusterpool;
   
    /*
     * (non-Javadoc)
     * @see com.alisoft.xplatform.asf.cache.ICacheManager#start()
     */
    public void init() {
        cachepool = new ConcurrentHashMap<String, IMemCached>();
        clusterpool = new ConcurrentHashMap<String, IMemCached>();
       // redisClusterpool = new ConcurrentHashMap<String, AbstractRedisDao>();
       // clusterConfigs = new ArrayList<MemcachedClientClusterConfig>();

        initManager();
        FailoverJob command = new FailoverJob(this);
        failoverThread.scheduleAtFixedRate(command, 5, 2, TimeUnit.SECONDS);
    }
    
    private DefaultDiamondManager diamondManager;
    private void initManager(){
		
		diamondManager = new DefaultDiamondManager(
				group,dataId, new ManagerListener() {
					public Executor getExecutor() {
						return null;
					}

					public void receiveConfigInfo(String newConfigInfo) {
						if (null == newConfigInfo) {
							return;
						}

						List<CacheClusterConfig> rs = CacheConfigurationPaser.loadMemcachedConfig (newConfigInfo);;
						try {
							updateClientPool(rs);
						} catch (Exception ex) {
							log.error("MemcachedManager init error ,please check !");
							throw new RuntimeException("MemcachedManager init error ,please check !",ex);
						}

					}

				});
		
		String configInfo = diamondManager.getAvailableConfigureInfomation(3000);
		if (configInfo != null) {
			List<CacheClusterConfig> clusterConfigs = CacheConfigurationPaser.loadMemcachedConfig(configInfo);

	        if (clusterConfigs != null &&  clusterConfigs.size() > 0) {
	            try {
	                initCacheClientPool(clusterConfigs);
	            } catch (Exception ex) {
	                log.error("MemcachedManager init error ,please check !");
	                throw new RuntimeException("MemcachedManager init error ,please check !", ex);
	            }

	        } else {
	            log.error("no config info for MemcachedManager,please check !");
	            throw new RuntimeException("no config info for MemcachedManager,please check !");
	        }
		}
	}
    
    
    
   

    /**
     * init resource pool
     */
	
	protected synchronized void initCacheClientPool(List<CacheClusterConfig> clusterConfigs) {
		for (CacheClusterConfig clusterCf : clusterConfigs) {
			initClusterCfg(clusterCf);
		}
	}

	private void initClusterCfg (CacheClusterConfig clusterCf ) {
		ICacheCluster cluster = null;
		if (CacheClusterConfig.REDIS_CLUSTER.equals(clusterCf.getClusterType())) {
			cluster = new  RedisCluster();
	         
		}else if (CacheClusterConfig.SSDB_CLUSTER.equals(clusterCf.getClusterType())) {
			cluster = new  SSDBCluster();
		} else {
			cluster = new MemcachedClientCluster();
		}
		cluster.setName(clusterCf.getName());
		
		cluster.setClusterAlg(clusterCf.getClusterAlg());
		ArrayList<IMemCached> cList = new ArrayList<IMemCached>();
		
		cluster.setFailover(clusterCf.isFailover());
		cluster.setCacheManager(this);
		cluster.setAsyn(clusterCf.isAsyn());

		for (CacheConfig ctCg : clusterCf.getClients()) {
			
			SocketPoolConfig scfg = ctCg.getSocketPool();
			IMemCached mc = null;
			
			if (CacheConfig.REDIS.equals(ctCg.getProtocol())) {
				mc = this.initRedisClient(ctCg);
			}else if (CacheConfig.SSDB.equals(ctCg.getProtocol())) {
				mc = this.initSSdbClient(ctCg);
			} else {
				this.initPool(ctCg);
				MemCachedClient client =  MemCachedClient.createInstance (scfg.getName(),ctCg.isTcp(), ctCg.isBinary());
				mc = new MemcachedCache(client);
				mc.setName(ctCg.getName());
				mc.setSlave(ctCg.isSlave());
				
			}
			mc.setWeight(ctCg.getWeight());
			cachepool.put(ctCg.getName(), mc);
			cList.add(mc);
		}

		cluster.setCaches(cList);
		if (clusterpool.get(cluster.getName()) != null)
			log.error(new StringBuilder("cluster define duplicate! cluster name :").append(cluster.getName()).toString());
         cluster.init();
         
		clusterpool.put(cluster.getName(),(IMemCached) cluster);
		
	}
	
	protected synchronized void updateClientPool(List<CacheClusterConfig> rs) {

		for (CacheClusterConfig clusterCf : rs) {
			boolean initNeed = false;
			Set<String> tmpSet = new HashSet<String>();
			ICacheCluster cluster =(ICacheCluster) clusterpool.get(clusterCf.getName());
			
			if (cluster == null) {
				//add a cluster
				initClusterCfg(clusterCf);
			} else {
				if (! clusterCf.getClusterAlg().equals(cluster.getClusterAlg())) {
					cluster.setClusterAlg(clusterCf.getClusterAlg());
					initNeed = true;
				}
				cluster.setFailover(clusterCf.isFailover());
				List<IMemCached> newCaches = new ArrayList<IMemCached> ();   //cluster.copyCaches();
				for (CacheConfig ctCg : clusterCf.getClients()) {
					SocketPoolConfig scfg = ctCg.getSocketPool();
					tmpSet.add(ctCg.getName());
					IMemCached mc = null;
					if (CacheConfig.REDIS.equals(ctCg.getProtocol())) {
						mc = this.initRedisClient(ctCg);
						if (cachepool.get(ctCg.getName()) != null) {
							mc.setName(ctCg.getName());
							cachepool.put(ctCg.getName(), mc);
							
						}
					 } else if (CacheConfig.SSDB.equals(ctCg.getProtocol())) {
							mc = this.initSSdbClient(ctCg);
							if (cachepool.get(ctCg.getName()) != null) {
								mc.setName(ctCg.getName());
								cachepool.put(ctCg.getName(), mc);
								
							}
						 } else {
							 this.initPool(ctCg);
							 mc = cachepool.get(ctCg.getName());
							 if (mc == null) {
								// add a client
								MemCachedClient client = MemCachedClient.createInstance(scfg.getName(),ctCg.isTcp(), ctCg.isBinary());
								mc = new MemcachedCache(client);
								mc.setName(ctCg.getName());
								cachepool.put(ctCg.getName(), mc);
							 }
							 
							 mc.setSlave(ctCg.isSlave());
					 }
					
					if (mc.getWeight() != ctCg.getWeight()) {
						mc.setWeight(ctCg.getWeight());
						initNeed = true;
					}
					newCaches.add(mc);
				}
				
				List<IMemCached> copyCaches = cluster.copyCaches();
				List<IMemCached> deadlist = new ArrayList<IMemCached>();
				//if (nameSet.size() != cluster.getClusterSet().size()) {
					//remove client
					for ( IMemCached mc: copyCaches) {
						if (tmpSet.add(mc.getName())) {
							deadlist.add(cachepool.remove(mc.getName()));
						} 
					}
				if (deadlist.size()>0) {
					cluster.setCaches(newCaches);
					destory(deadlist);
					deadlist.clear();
					deadlist=null;
					log.info("+++++ remove client success ");
				} else if (newCaches.size() > copyCaches.size()) {
					cluster.setCaches(newCaches);
					deadlist=null;
					copyCaches.clear();
					newCaches=null;
					copyCaches=null;
				}
				
				if (initNeed)
					cluster.init();
			}
		}
	}
    
	public void destory(Collection<IMemCached> deadlist) {
		for (IMemCached c:deadlist) {
			try {
			c.close();
			}catch (Exception e) {
				log.error("close cache ",e);
			}
			log.info("+++++ remove client" + c.getName() );
		}
	}
   
    private void initPool ( CacheConfig cfg ) {
    	SocketPoolConfig socketPool = cfg.getSocketPool();
        if (socketPool.getServers() != null && !socketPool.getServers().equals("")) {
            SchoonerSockIOPool pool = SchoonerSockIOPool.getInstance(socketPool.getName());
        	PoolConfig config = new PoolConfig();
        	config.setTcp(socketPool.isTcp());
            String[] servers = socketPool.getServers().split(",");
            String[] weights = null;
            Integer[] weightsarr=null;
            if (socketPool.getWeights() != null && !socketPool.getWeights().equals("")) weights = socketPool.getWeights().split(",");
            
            if (weights != null && weights.length > 0 && weights.length == servers.length) {
                 weightsarr = new Integer[weights.length];

                for (int i = 0; i < weights.length; i++)
                    weightsarr[i] = new Integer(weights[i]);
            }
            

            if (socketPool.getInitConn() > 0) config.setInitialSize(socketPool.getInitConn());
            if (socketPool.getMinIdle() > 0) config.setMinIdle(socketPool.getMinIdle());
            if (socketPool.getTimeBetweenEvictionRunsMillis() > 0) config.setTimeBetweenEvictionRunsMillis(socketPool.getTimeBetweenEvictionRunsMillis());
            if (socketPool.getMinEvictableIdleTimeMillis() > 0) config.setMinEvictableIdleTimeMillis(socketPool.getMinEvictableIdleTimeMillis());
            if (socketPool.getNumTestsPerEvictionRun() > 0) config.setNumTestsPerEvictionRun(socketPool.getNumTestsPerEvictionRun());
            
            if (socketPool.getTimeout() > 0) config.setTimeout(socketPool.getTimeout());
            if (socketPool.getMaxActive() > 0) {
            	config.setMaxIdle(socketPool.getMaxActive());
            	config.setMaxActive(socketPool.getMaxActive());
            }
            if (socketPool.getConnectTimeout() > 0) config.setConnectTimeout(socketPool.getConnectTimeout());
            if (socketPool.getMaxWait() > 0) config.setMaxWait(socketPool.getMaxWait());
            
            pool.setMaxBusy(socketPool.getMaxBusyTime());
            pool.setPoolName(socketPool.getName());
            config.setNagle(socketPool.isNagle());
            pool.setFailover(socketPool.isFailover());
            
            config.setTestOnBorrow(socketPool.isTestOnBorrow());
            
            if (! pool.isInited()){
            	pool.setHashingAlg(socketPool.getHashingAlg());
            	pool.setServers(servers);
            	pool.setWeights(weightsarr);
            	pool.setConfig(config);
            	pool.initialize();
            	
            } else {
            	pool.updatePool(servers, socketPool.getHashingAlg(),weightsarr,config);
            }
        } else {
            log.error("MemcachedClientSocketPool config error !");
            throw new RuntimeException("MemcachedClientSocketPool config error !");
        }
    }
    
    private AbstractRedisDao initRedisClient (CacheConfig cfg) {
    	SocketPoolConfig socketPool =  cfg.getSocketPool();
        if (socketPool.getServers() != null && !socketPool.getServers().equals("")) {
        	AbstractRedisDao pool = (AbstractRedisDao) cachepool.get(socketPool.getName());
            JedisPoolConfig config = new JedisPoolConfig();
        	//config.setTcp(socketPool.isTcp());
            String[] servers = socketPool.getServers().split(",");
            String[] weights = null;
            int [] weightsarr=null;
            if (socketPool.getWeights() != null && !socketPool.getWeights().equals("")) weights = socketPool.getWeights().split(",");
            
            if (weights != null && weights.length > 0 && weights.length == servers.length) {
                 weightsarr = new int[weights.length];

                for (int i = 0; i < weights.length; i++)
                    weightsarr[i] =  Integer.parseInt(weights[i]);
            }
            
           // if (socketPool.getInitConn() > 0) config.setInitialSize(socketPool.getInitConn());
            if (socketPool.getMinIdle() > 0) config.setMinIdle(socketPool.getMinIdle());
            if (socketPool.getTimeBetweenEvictionRunsMillis() > 0) config.setTimeBetweenEvictionRunsMillis(socketPool.getTimeBetweenEvictionRunsMillis());
            if (socketPool.getMinEvictableIdleTimeMillis() > 0) config.setMinEvictableIdleTimeMillis(socketPool.getMinEvictableIdleTimeMillis());
            if (socketPool.getNumTestsPerEvictionRun() > 0) config.setNumTestsPerEvictionRun(socketPool.getNumTestsPerEvictionRun());
            
           // if (socketPool.getTimeout() > 0) config.setTimeout(socketPool.getTimeout());
            if (socketPool.getMaxActive() > 0) {
            	config.setMaxIdle(socketPool.getMaxActive());
            	config.setMaxActive(socketPool.getMaxActive());
            }
           // if (socketPool.getConnectTimeout() > 0) config.setConnectTimeout(socketPool.getConnectTimeout());
            if (socketPool.getMaxWait() > 0) config.setMaxWait(socketPool.getMaxWait());
            
           // pool.setMaxBusy(socketPool.getMaxBusyTime());
           
           // config.setNagle(socketPool.isNagle());
           // pool.setFailover(socketPool.isFailover());
            
            config.setTestOnBorrow(socketPool.isTestOnBorrow());
            
            if (pool == null){
            	//pool.setHashingAlg(socketPool.getHashingAlg());
            	if (cfg.isSentinel()) {
            		pool = new SentinelRedisCache(socketPool.getName(),servers,cfg.getSentinelName(),config,(socketPool.getTimeout() > 0)?socketPool.getTimeout():3000);
            	} else {
            		pool = new RedisCache(socketPool.getName(),servers, weightsarr, config,(socketPool.getTimeout() > 0)?socketPool.getTimeout():3000);	
            		if (cfg.isSlave()) {
            			pool.setSlave(cfg.isSlave());
            		}
            	}
            } else {
            	pool.updatePool(servers, socketPool.getHashingAlg(),weightsarr, config, (socketPool.getTimeout() > 0)?socketPool.getTimeout():3000);
            	
            }
            
            return pool;
        } else {
            log.error("MemcachedClientSocketPool config error !" + socketPool.getName());
            throw new RuntimeException("MemcachedClientSocketPool config error !" + socketPool.getName());
        }
    }
    
    private AbstractSSDBDao initSSdbClient (CacheConfig cfg) {
    	SocketPoolConfig socketPool =  cfg.getSocketPool();
        if (socketPool.getServers() != null && !socketPool.getServers().equals("")) {
        	SSDBCache pool = (SSDBCache) cachepool.get(socketPool.getName());
            PoolConfig config = new PoolConfig();
        	//config.setTcp(socketPool.isTcp());
            String[] servers = socketPool.getServers().split(",");
            String[] weights = null;
            int [] weightsarr=null;
            if (socketPool.getWeights() != null && !socketPool.getWeights().equals("")) weights = socketPool.getWeights().split(",");
            
            if (weights != null && weights.length > 0 && weights.length == servers.length) {
                 weightsarr = new int[weights.length];

                for (int i = 0; i < weights.length; i++)
                    weightsarr[i] =  Integer.parseInt(weights[i]);
            }
            
            if (socketPool.getInitConn() > 0) config.setInitialSize(socketPool.getInitConn());
            if (socketPool.getMinIdle() > 0) config.setMinIdle(socketPool.getMinIdle());
            if (socketPool.getTimeBetweenEvictionRunsMillis() > 0) config.setTimeBetweenEvictionRunsMillis(socketPool.getTimeBetweenEvictionRunsMillis());
            if (socketPool.getMinEvictableIdleTimeMillis() > 0) config.setMinEvictableIdleTimeMillis(socketPool.getMinEvictableIdleTimeMillis());
            if (socketPool.getNumTestsPerEvictionRun() > 0) config.setNumTestsPerEvictionRun(socketPool.getNumTestsPerEvictionRun());
            
            if (socketPool.getTimeout() > 0) config.setTimeout(socketPool.getTimeout());
            if (socketPool.getMaxActive() > 0) {
            	config.setMaxIdle(socketPool.getMaxActive());
            	config.setMaxActive(socketPool.getMaxActive());
            }
            if (socketPool.getConnectTimeout() > 0) config.setConnectTimeout(socketPool.getConnectTimeout());
            if (socketPool.getMaxWait() > 0) config.setMaxWait(socketPool.getMaxWait());
            
           // pool.setMaxBusy(socketPool.getMaxBusyTime());
           
           // config.setNagle(socketPool.isNagle());
           // pool.setFailover(socketPool.isFailover());
            
            config.setTestOnBorrow(socketPool.isTestOnBorrow());
            
            if (pool == null){
            	pool = new SSDBCache (socketPool.getName(),servers,weightsarr,config,socketPool.getHashingAlg());
            	
            } else {
            	pool.updatePool(servers, socketPool.getHashingAlg(),weightsarr, config);	
            }
            
			if (cfg.isSlave()) {
				pool.setSlave(cfg.isSlave());
			}
            
            return pool;
        } else {
            log.error("+++SocketPool config error !" + socketPool.getName());
            throw new RuntimeException("+++SocketPool config error !" + socketPool.getName());
        }
    }
    
    
    public IMemCached getCache(String name) {
        return getCachepool().get(name);
    }

    public IMemCached getCacheCluster(String name) {
        return getClusterpool().get(name);
    }
    
    @Override
	public IRedisDao getRedisCacheCluster(String name) {
		
		return (IRedisDao)clusterpool.get(name);
	}


    @Override
	public ISSDBDao getSSDBCluster(String name) {
		
		return (ISSDBDao)clusterpool.get(name);
	}
    
    public ConcurrentHashMap<String, IMemCached> getCachepool() {
        if (cachepool == null) throw new java.lang.RuntimeException("cachepool is null!");

        return cachepool;
    }

    public ConcurrentHashMap<String, IMemCached> getClusterpool() {
        if (clusterpool == null) throw new java.lang.RuntimeException("clusterpool is null!");

        return clusterpool;
    }
    
    public static class FailoverJob implements Runnable  {
    	private DiamondConfiguration mc = null;
    	
		public FailoverJob(DiamondConfiguration mc) {
			super();
			this.mc = mc;
		}

		@Override
		public void run() {
			for (IMemCached im: mc.getClusterpool().values()) {
				if (log.isDebugEnabled()){
					log.debug("++++++ pingCheck ============================");
				} 
				if(! im.pingCheck()) {
					log.error("++++++ pingCheck  error for cluster " + im.getName());
				}
				
			}
			
		}
    	
    }
}
