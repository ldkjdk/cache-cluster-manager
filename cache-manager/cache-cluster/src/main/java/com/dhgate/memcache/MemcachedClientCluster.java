package com.dhgate.memcache;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dhgate.CacheClusterConfig;
import com.dhgate.ConsistentHash;
import com.dhgate.ICachedConfigManager;
import com.dhgate.RoutingAlg;
import com.dhgate.WeightedRoundRobinScheduling;
import com.dhgate.memcache.schooner.NativeHandler;
import com.dhgate.redis.ObjectBytesExchange;

/**
 * 
 * 
 * @author lidingkun
 */
public class MemcachedClientCluster implements IMemCached,ICacheCluster {
	private static final Logger log          = LoggerFactory.getLogger(MemcachedClientCluster.class);

    private String            name;
    private String clusterAlg;
    private boolean failover = true;
    

    private List<IMemCached>  caches;
    private Map<String,IMemCached>  badCaches = null ;
   
    private ICachedConfigManager cacheManager;
    private RoutingAlg<IMemCached> alg;
    
   
    private boolean           isAsyn  = true;
    private ClusterProcessorManager invoke = ClusterProcessorManager.newInstance();
    
    public boolean isAsyn() {
        return isAsyn;
    }

    public void setAsyn(boolean isAsyn) {
        this.isAsyn = isAsyn;
    }

    public MemcachedClientCluster(){

    }
    @Override
    public void init () {
    	
    	if (CacheClusterConfig.ALG_HASH.equals(this.clusterAlg)) {
    		int weights [] = new int [caches.size()];
    		Map<String,IMemCached> map = new HashMap<String,IMemCached>();
    		for (int i=0;i<caches.size();i++) {
    			weights[i] = caches.get(i).getWeight();
    			map.put(caches.get(i).getName(), caches.get(i));
    		}
    		alg = new ConsistentHash<IMemCached>(weights, map);
		} else {

			int weights[] = new int[caches.size()];
			for (int i = 0; i < caches.size(); i++) {
				weights[i] = caches.get(i).getWeight();
			}
			
			alg = new WeightedRoundRobinScheduling<IMemCached>(weights, caches);
		}
	}
    @Override
    public String getName() {
        return name;
    }
    @Override
    public void setName(String name) {
        this.name = name;
    }
    @Override
    public List<IMemCached> copyCaches() {
    	if (this.caches == null) return null;
		 ArrayList<IMemCached> tmp = new ArrayList<IMemCached> ();
		 for (IMemCached ch:caches) {
			 tmp.add(ch);
		 }
		return tmp;
    }
    @Override
    public void setCaches(List<IMemCached> caches) {
    	if (caches == null || caches.size() <=0) throw new RuntimeException("setCaches caches is null");
        this.caches = caches;
        this.init();
        if (badCaches != null && badCaches.size() > 0) {
        	this.cacheManager.destory(badCaches.values());
        	badCaches.clear();
        } else {
        	badCaches = new HashMap<String,IMemCached>();
        } 
    }

   
    @Override
	public ICachedConfigManager getCacheManager() {
        return cacheManager;
    }
	@Override
    public void setCacheManager(ICachedConfigManager cacheManager) {
        this.cacheManager = cacheManager;
    }
    @Override
    public boolean add(final String key, final Object v) {
    	if (key == null || v == null) return false;
    	
    	final Object value = copyObject(v);
        boolean falg = false;
        final Iterator<IMemCached> iterator = caches.iterator();
        while (iterator.hasNext()) {
			IMemCached dao = iterator.next();
			if (dao.isSlave()) {
				continue;
			}
			falg = dao.add(key, value);
			if (this.isAsyn) {
				break;
            }
        }
        
        if (iterator.hasNext()) {
			Runnable run = new Runnable() {
				@Override
				public void run() {
					while (iterator.hasNext()) {
						IMemCached dao = iterator.next();
						if (dao.isSlave()) continue;
						dao.add(key, value);
					}
				}
			};
			this.invoke.exec(run);
		} 
        return falg;
    }
    @Override
    public boolean add(final String key, Object v, final Date expiry) {
    	if (key == null || v == null) return false;
    	final Object value= copyObject(v);
        boolean falg = false;
        final Iterator<IMemCached> iterator = caches.iterator();
        while (iterator.hasNext()) {
            IMemCached dao = iterator.next();
			if (dao.isSlave()) {
				continue;
			}
			falg = dao.add(key, value, expiry);
			if (this.isAsyn) {
				break;
			}
        }
        
        if (iterator.hasNext()) {
			Runnable run = new Runnable() {
				@Override
				public void run() {
					while (iterator.hasNext()) {
						IMemCached dao = iterator.next();
						if (dao.isSlave()) continue;
						dao.add(key, value, expiry);
					}
				}
			};
			this.invoke.exec(run);
		} 
        
        return falg;
    }

   
    /**
     * 同步删除、只要有一个没有删除成功，就算没有成功；
     * 
     * @param key key
     */
    @Override
    public boolean delete(String key) {
        boolean falg = true;
        Iterator<IMemCached> iterator = caches.iterator();
        while (iterator.hasNext()) {
            IMemCached memCached = iterator.next();
            if (!memCached.delete(key)) {
                falg = false;
                // 删除还是同步删除，才能确定是否全部删除
                // if (this.isAsyn)
                // {
                // Object[] commands = new Object[]{iterator,DELETE,key};
                // ClusterProcessorManager.newInstance().asynProcess(commands);
                // return falg;
                // }
            }
        }
        return falg;
    }


	@Override
	public boolean flushAll() {
		 boolean falg = false;
	        Iterator<IMemCached> iterator = caches.iterator();
	        while (iterator.hasNext()) {
	            IMemCached dao = iterator.next();
	            falg=dao.flushAll();
	        }
	        return falg;
	}

	@Override
	public boolean flushAll(String[] servers) {
		boolean falg = false;
        Iterator<IMemCached> iterator = caches.iterator();
        while (iterator.hasNext()) {
            IMemCached dao = iterator.next();
            falg=dao.flushAll(servers);
        }
        return falg;
	}
    
   
    @Override
    public Object get(String key) {
    	if (CacheClusterConfig.ALG_POLLING.equals(this.clusterAlg)) {
    		IMemCached memCached = this.alg.getServerObject();
    		if (log.isDebugEnabled()) {
    			log.debug("clusterAlg==" + this.clusterAlg + " cacheName=" + memCached.getName());
    		}
    		return memCached.get(key);
    		
    	} else if (CacheClusterConfig.ALG_HASH.equals(this.clusterAlg)) {
    		IMemCached memCached = this.alg.getServerObject(key);
    		if (log.isDebugEnabled()) {
    			log.debug("clusterAlg==" + this.clusterAlg + " cacheName=" + memCached.getName());
    		}
    		return memCached.get(key);
    	} else {
    		List<IMemCached> list = this.alg.getAllServerObject();
    		int index = this.alg.getServerIndex();
    		IMemCached memCached = list.get(index);
    		Object falg =  memCached.get(key);
    		if (null != falg) {
                return falg;
            }
    		for (int i=0;i<list.size();i++) {
    			if (i != index) {
    		        memCached = list.get(i);
    		        falg =  memCached.get(key);
    	    		if (null != falg) {
    	                return falg;
    	            }
    			}
    		}
    		
    		/*Iterator<IMemCached> iterator = caches.iterator();
	        while (iterator.hasNext()) {
	            IMemCached memCached = iterator.next();
	            if (memCached.getName().equals(anObject))
	             falg = memCached.get(key);
	            if (null != falg) {
	                return falg;
	            }
	        }*/
    	}
    	
        return null;
    }
    @Override
    public Map<String, Object> getMulti(String[] keys) {

    	if (CacheClusterConfig.ALG_POLLING.equals(this.clusterAlg)) {
    		IMemCached memCached = this.alg.getServerObject();
    		return memCached.getMulti(keys);
    		
    	} else if (CacheClusterConfig.ALG_HASH.equals(this.clusterAlg)) {
    		IMemCached memCached = this.alg.getServerObject(keys[0]);
    		return memCached.getMulti(keys);
		} else {
			Iterator<IMemCached> iterator = caches.iterator();
			while (iterator.hasNext()) {
				IMemCached memCached = iterator.next();
				Map<String, Object> result = memCached.getMulti(keys);
				if (null != result) {
					return result;
				}
			}
		}
        return null;
    }
    @Override
    public Object[] getMultiArray(String[] keys) {
    	if (CacheClusterConfig.ALG_POLLING.equals(this.clusterAlg)) {
    		IMemCached memCached = this.alg.getServerObject();
    		return memCached.getMultiArray(keys);
    		
    	} else if (CacheClusterConfig.ALG_HASH.equals(this.clusterAlg)) {
    		IMemCached memCached = this.alg.getServerObject(keys[0]);
    		return memCached.getMultiArray(keys);
		}
    	
        Iterator<IMemCached> iterator = caches.iterator();
        while (iterator.hasNext()) {
            IMemCached memCached = iterator.next();
            Object[] result = memCached.getMultiArray(keys);
            if (null != result) {
                return result;
            }
        }
        return null;
    }
    @Override
    public boolean keyExists(String key) {

    	if (CacheClusterConfig.ALG_POLLING.equals(this.clusterAlg)) {
    		IMemCached memCached = this.alg.getServerObject();
    		return memCached.keyExists(key);
    		
    	} else if (CacheClusterConfig.ALG_HASH.equals(this.clusterAlg)) {
    		IMemCached memCached = this.alg.getServerObject(key);
    		return memCached.keyExists(key);
    	} 
    	
        Iterator<IMemCached> iterator = caches.iterator();
        while (iterator.hasNext()) {
            IMemCached memCached = iterator.next();
            if (memCached.keyExists(key)) {
                return true;
            }
        }
        return false;
    }

    

    /**
     * 返回的是只要有一个替换成功就算成功； 只要有一个替换成功，其他的走异步替换；
     */
    @Override
    public boolean replace(final String key, Object v) {
    	if (key == null || v == null) return false;
    	final Object value= copyObject(v);
        boolean falg = false;
        final Iterator<IMemCached> iterator = caches.iterator();
        while (iterator.hasNext()) {
            IMemCached dao = iterator.next();
            if (dao.isSlave()) continue;
			falg = dao.replace(key, value);
            if (this.isAsyn) break;
        }
        
        if (iterator.hasNext()) {
			Runnable run = new Runnable() {
				@Override
				public void run() {
					while (iterator.hasNext()) {
						IMemCached dao = iterator.next();
						if (dao.isSlave()) continue;
						dao.replace(key, value);
					}
				}
			};
			this.invoke.exec(run);
		} 
        
        return falg;
    }
    @Override
    public boolean replace(final String key, Object v, final Date expiry) {
    	if (key == null || v == null) return false;
        boolean falg = false;
        final Object value= copyObject(v);
        final Iterator<IMemCached> iterator = caches.iterator();
        while (iterator.hasNext()) {
        	IMemCached dao = iterator.next();
            if (dao.isSlave()) continue;
			falg = dao.replace(key, value, expiry);
            if (this.isAsyn) break;
        }
        
        if (iterator.hasNext()) {
			Runnable run = new Runnable() {
				@Override
				public void run() {
					while (iterator.hasNext()) {
						IMemCached dao = iterator.next();
						if (dao.isSlave()) continue;
						dao.replace(key, value, expiry);
					}
				}
			};
			this.invoke.exec(run);
		} 
        return falg;
    }
    @Override
    public boolean set(final String key, Object v) {
    	if (key == null || v == null) return false;
    	boolean falg = false;
        final Object value= copyObject(v);
        final Iterator<IMemCached> iterator = caches.iterator();
        while (iterator.hasNext()) {
        	IMemCached dao = iterator.next();
            if (dao.isSlave()) continue;
			falg = dao.set(key, value);
            if (this.isAsyn) break;
        }
        
        if (iterator.hasNext()) {
			Runnable run = new Runnable() {
				@Override
				public void run() {
					while (iterator.hasNext()) {
						IMemCached dao = iterator.next();
						if (dao.isSlave()) continue;
						dao.set(key, value);
					}
				}
			};
			this.invoke.exec(run);
		} 
        return falg;
    }
    @Override
    public boolean set(final String key, Object v, final Date expiry) {
    	if (key == null || v == null) return false;
    	boolean falg = false;
        final Object value= copyObject(v);
        final Iterator<IMemCached> iterator = caches.iterator();
        while (iterator.hasNext()) {
        	IMemCached dao = iterator.next();
            if (dao.isSlave()) continue;
			falg = dao.set(key, value,expiry);
            if (this.isAsyn) break;
        }
        
        if (iterator.hasNext()) {
			Runnable run = new Runnable() {
				@Override
				public void run() {
					while (iterator.hasNext()) {
						IMemCached dao = iterator.next();
						if (dao.isSlave()) continue;
						dao.set(key, value,expiry);
					}
				}
			};
			this.invoke.exec(run);
		} 
        return falg;
    }
    @Override
    public boolean add(String key, Object value, long liveTime) {
        return add(key, value, new Date(liveTime * 1000));
    }

    // public boolean delete(String key, long liveTime) {
    // return delete(key, new Date(liveTime * 60 * 1000));
    // }
    @Override
    public boolean replace(String key, Object value, long liveTime) {
        return replace(key, value, new Date(liveTime * 1000));
    }
    @Override
    public boolean set(String key, Object value, long liveTime) {
        return set(key, value, new Date(liveTime * 1000));
    }

	@Override
	public void close() {

		for(IMemCached c: caches) {
			c.close();
		}
	}

	@Override
	public String getClusterAlg() {
		return clusterAlg;
	}

	@Override
	public void setClusterAlg(String clusterAlg) {
		this.clusterAlg = clusterAlg;
	}

	@Override
	public boolean isFailover() {
		return failover;
	}

	@Override
	public void setFailover(boolean failover) {
		this.failover = failover;
	}

	@Override
	public int getWeight() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean pingCheck() {
		if (this.failover == false) return true;
		List<IMemCached> tmp = new ArrayList<IMemCached> ();
		
		for (IMemCached ch: caches) {
			
			if ( ping(ch) ) {
				tmp.add(ch);
			} else {
				this.badCaches.put(ch.getName(),ch);
			}
		}
		
		if (badCaches.size() <= 0) {
			tmp = null;
			return true;
		} else {
			for (IMemCached ch: badCaches.values()) {
			
				if (ping(ch)) {
					tmp.add(ch);
				} else {
					int trycn=0;
					while (trycn <PING_CN) {
						if (ping(ch)) {
							tmp.add(ch);
							break;
						}
						try {
							Thread.sleep(100);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						trycn ++;
					}
					
				}
			}
			
			if (! compareCaches(tmp)) {
				this.caches=tmp;
				this.init();
				for (IMemCached ch:tmp) {
					badCaches.remove(ch.getName());
				}
				log.info("++++++++++++++ failover for cluster  " + this.getName());
			}
		}
		tmp = null;
		return false;
	}
    private boolean compareCaches(List<IMemCached> tmp) {
    	if (tmp.size() != caches.size()) return false;
    	for(IMemCached ch:caches) {
    		boolean f = false;
    		for (IMemCached t:tmp) {
    			if (ch.getName().equals(t.getName())) {
    				f = true;
    				break;
    			}
    		}
    		
    		if (!f)
    			return f;
    	}
    	
    	return true;
    }
	
	private boolean ping (IMemCached ch) {
		boolean r = true;
		try {
			 r = ch.pingCheck();
		}catch (Exception e) {
			log.error("pingCheck",e);
			r=false;
		}
		
		return r;
	}
	@Override
	public void setWeight(int weight) {
		// TODO Auto-generated method stub
		
	}
	
	
	private Object copyObject (Object value) {
		if (NativeHandler.isHandled( value )) {
			return value;
		} else {
			byte[] byteObj = ObjectBytesExchange.toByteArray(value);
			return ObjectBytesExchange.toObject(byteObj);
		}
	}

	@Override
	public boolean isSlave() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void setSlave(boolean isSlave) {
		// TODO Auto-generated method stub
		
	}

}
