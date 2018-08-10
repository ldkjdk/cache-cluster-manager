package com.dhgate.redis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dhgate.CacheClusterConfig;
import com.dhgate.ConsistentHash;
import com.dhgate.ICachedConfigManager;
import com.dhgate.RoutingAlg;
import com.dhgate.WeightedRoundRobinScheduling;
import com.dhgate.memcache.ClusterProcessorManager;
import com.dhgate.memcache.ICacheCluster;
import com.dhgate.memcache.IMemCached;
import com.dhgate.redis.clients.jedis.JedisPubSub;

public class RedisCluster extends AbstractRedisDao implements ICacheCluster{

    private static final Logger log          = LoggerFactory.getLogger(AbstractRedisDao.class);
	 
    private String            name;
	private String clusterAlg;
	private boolean failover = true;

	private List<AbstractRedisDao> caches;
	private Map<String, IMemCached> badCaches = null;
	
	private ICachedConfigManager cacheManager;
	private RoutingAlg<AbstractRedisDao> alg;
	private boolean isAsyn  = true;
	
	private ClusterProcessorManager invoke = ClusterProcessorManager.newInstance();
	
	@Override
	public String setString(final String key, final String str, final int liveSeconds,final int index) {
		
		String ret = null;
		final Iterator<AbstractRedisDao> iterator = caches.iterator();
		while (iterator.hasNext()) {
			AbstractRedisDao dao = iterator.next();
			if (dao.isSlave())
				continue;
			ret = dao.setString(key, str, liveSeconds, index);
			if (this.isAsyn)
				break;
		}
		if (iterator.hasNext()) {
			Runnable run = new Runnable() {
				@Override
				public void run() {
					while (iterator.hasNext()) {
						AbstractRedisDao dao = iterator.next();
						if (dao.isSlave()) continue;
						dao.setString(key, str, liveSeconds, index);
					}
				}
			};
			this.invoke.exec(run);
		}
		return ret;
	}

	@Override
	public String getString(String key, int index) {
		String r=null;
		if (CacheClusterConfig.ALG_POLLING.equals(this.clusterAlg)) {
			AbstractRedisDao dao = this.alg.getServerObject();
			r = dao.getString(key, index);
    		if (log.isDebugEnabled()) {
    			log.debug("clusterAlg==" + this.clusterAlg + " Reids client " + dao.getName());
    		}	
    		
    	} else if (CacheClusterConfig.ALG_HASH.equals(this.clusterAlg)) {
    		AbstractRedisDao dao = this.alg.getServerObject(key);
    		r = dao.getString(key, index);
    		if (log.isDebugEnabled()) {
    			log.debug("clusterAlg==" + this.clusterAlg + " Reids client " + dao.getName());
    		}	
    	} else {
    		List<AbstractRedisDao> list = this.alg.getAllServerObject();
    		int si = this.alg.getServerIndex();
    		AbstractRedisDao dao = list.get(si);
    		r = dao.getString(key, index);
    		if (null != r) {
                return r;
            }
    		for (int i=0;i<list.size();i++) {
    			if (i != si) {
    				dao = list.get(i);
    	    		r = dao.getString(key, index);
    	    		if (null != r) {
    	                break;
    	            }
    			}
    		}
    		
    		
    	}
		
		return r;
	}

	@Override
	public boolean delKeyByBatch(final List<String> keyList, final int index) {
		boolean ret = false;
		final Iterator<AbstractRedisDao> iterator = caches.iterator();
		while (iterator.hasNext()) {
			AbstractRedisDao dao = iterator.next();
			if (dao.isSlave())
				continue;
			ret = dao.delKeyByBatch(keyList, index);
			//break;
		}
		
		/*if (iterator.hasNext()) {
			Runnable run = new Runnable() {
				@Override
				public void run() {
					while (iterator.hasNext()) {
						AbstractRedisDao dao = iterator.next();
						if (dao.isSlave()) continue;
						dao.delKeyByBatch(keyList, index);
					}
				}
			};
			this.invoke.exec(run);
		}*/
		return ret;
	}

	@Override
	public boolean isExist(String key, int index) {
		boolean r = false;
		if (CacheClusterConfig.ALG_POLLING.equals(this.clusterAlg)) {
			AbstractRedisDao dao = this.alg.getServerObject();
			r = dao.isExist(key, index);
    		if (log.isDebugEnabled()) {
    			log.debug("clusterAlg==" + this.clusterAlg + " Reids client " + dao.getName());
    		}	
    		
    	} else if (CacheClusterConfig.ALG_HASH.equals(this.clusterAlg)) {
    		AbstractRedisDao dao = this.alg.getServerObject(key);
    		r = dao.isExist(key, index);
    		if (log.isDebugEnabled()) {
    			log.debug("clusterAlg==" + this.clusterAlg + " Reids client " + dao.getName());
    		}	
    	} else {
    		
    		List<AbstractRedisDao> list = this.alg.getAllServerObject();
    		int si = this.alg.getServerIndex();
    		AbstractRedisDao dao = list.get(si);
    		r = dao.isExist(key, index);
    		if ( r) {
                return r;
            }
    		for (int i=0;i<list.size();i++) {
    			if (i != si) {
    				dao = list.get(i);
    	    		r = dao.isExist(key, index);
    	    		if (r) {
    	                break;
    	            }
    			}
    		}
    		
    		/*final Iterator<AbstractRedisDao> iterator = caches.iterator();
    		while (iterator.hasNext()) {
    			r = iterator.next().isExist(key, index);
    			if (r) break;
    		}*/
    	}
		
		return r;
	}

	
	@Override
	public Object eval(final String script, final List<String> keys, final List<String> args) throws Exception{
		Object ret = null;
		final Iterator<AbstractRedisDao> iterator = caches.iterator();
		
		while (iterator.hasNext()) {
			AbstractRedisDao dao = iterator.next();
			if (dao.isSlave())
				continue;
			ret = dao.eval(script, keys, args);
			/*if (this.isAsyn)
				break;*/
		}
		
		/*if (iterator.hasNext()) {
			Runnable run = new Runnable() {
				@Override
				public void run() {
					while (iterator.hasNext()) {
						AbstractRedisDao dao = iterator.next();
						if (dao.isSlave()) continue;
						 try {
							dao.eval(script, keys, args);
						} catch (Exception e) {
							log.error("eval",e);
						}
					}
				}
			};
			this.invoke.exec(run);
		}*/
		return ret;
	}

	@Override
	public long incr(final String key, final int index) {
		long ret = 0;
		final Iterator<AbstractRedisDao> iterator = caches.iterator();
		while (iterator.hasNext()) {
			AbstractRedisDao dao = iterator.next();
			if (dao.isSlave())
				continue;
			ret = dao.incr(key, index);
			if (this.isAsyn)
				break;
		}
		if (iterator.hasNext()) {
			Runnable run = new Runnable() {
				@Override
				public void run() {
					while (iterator.hasNext()) {
						AbstractRedisDao dao = iterator.next();
						if (dao.isSlave()) continue;
						    dao.incr(key, index);
					}
				}
			};
			this.invoke.exec(run);
		}
		return ret;
	}

	@Override
	public long expire(final String key, final int seconds,final int index) {
		long ret = 0;
		final Iterator<AbstractRedisDao> iterator = caches.iterator();
		while (iterator.hasNext()) {
			AbstractRedisDao dao = iterator.next();
			if (dao.isSlave())
				continue;
			ret = dao.expire(key, index);
			if (this.isAsyn)
				break;
		}
		
		if (iterator.hasNext()) {
			Runnable run = new Runnable() {
				@Override
				public void run() {
					while (iterator.hasNext()) {
						AbstractRedisDao dao = iterator.next();
						if (dao.isSlave()) continue;
						    dao.expire(key, index);
					}
				}
			};
			this.invoke.exec(run);
		}
		return ret;
	}

	@Override
	public long expireAt(final String key, final long unixTime, final int index) {
		long ret = 0;
		final Iterator<AbstractRedisDao> iterator = caches.iterator();
		while (iterator.hasNext()) {
			AbstractRedisDao dao = iterator.next();
			if (dao.isSlave())
				continue;
			ret = dao.expireAt(key, unixTime,index);
			if (this.isAsyn)
				break;
		}
		if (iterator.hasNext()) {
			Runnable run = new Runnable() {
				@Override
				public void run() {
					while (iterator.hasNext()) {
						AbstractRedisDao dao = iterator.next();
						if (dao.isSlave()) continue;
						    dao.expireAt(key,unixTime, index);
					}
				}
			};
			this.invoke.exec(run);
		}
		
		return ret;
	}


	@Override
	public long getHashLength(String key, int index) {
		long r=0;
		if (CacheClusterConfig.ALG_POLLING.equals(this.clusterAlg)) {
			AbstractRedisDao dao = this.alg.getServerObject();
			r = dao.getHashLength(key, index);
    		if (log.isDebugEnabled()) {
    			log.debug("clusterAlg==" + this.clusterAlg + " Reids client " + dao.getName());
    		}	
    		
    	} else if (CacheClusterConfig.ALG_HASH.equals(this.clusterAlg)) {
    		AbstractRedisDao dao = this.alg.getServerObject(key);
    		r = dao.getHashLength(key, index);
    		if (log.isDebugEnabled()) {
    			log.debug("clusterAlg==" + this.clusterAlg + " Reids client " + dao.getName());
    		}	
    	} else {
    		List<AbstractRedisDao> list = this.alg.getAllServerObject();
    		int si = this.alg.getServerIndex();
    		AbstractRedisDao dao = list.get(si);
    		r = dao.getHashLength (key, index);
    		if ( r > 0) {
                return r;
            }
    		for (int i=0;i<list.size();i++) {
    			if (i != si) {
    				dao = list.get(i);
    	    		r = dao.getHashLength(key, index);
    	    		if (r > 0) {
    	                break;
    	            }
    			}
    		}
    		
    		/*final Iterator<AbstractRedisDao> iterator = caches.iterator();
    		while (iterator.hasNext()) {
    			r = iterator.next().getHashLength(key, index);
    			if (r > 0) break;
    		}*/
    	}
		
		return r;
	}

	@Override
	public boolean isHashFieldExist(String key, String field, int index) {
		boolean r=false;
		if (CacheClusterConfig.ALG_POLLING.equals(this.clusterAlg)) {
			AbstractRedisDao dao = this.alg.getServerObject();
			r = dao.isHashFieldExist(key, field,index);
    		if (log.isDebugEnabled()) {
    			log.debug("clusterAlg==" + this.clusterAlg + " Reids client " + dao.getName());
    		}	
    		
    	} else if (CacheClusterConfig.ALG_HASH.equals(this.clusterAlg)) {
    		AbstractRedisDao dao = this.alg.getServerObject(key);
    		r = dao.isHashFieldExist(key, field,index);
    		if (log.isDebugEnabled()) {
    			log.debug("clusterAlg==" + this.clusterAlg + " Reids client " + dao.getName());
    		}	
    	} else {
    		List<AbstractRedisDao> list = this.alg.getAllServerObject();
    		int si = this.alg.getServerIndex();
    		AbstractRedisDao dao = list.get(si);
    		r = dao.isHashFieldExist(key, field,index);
    		if ( r) {
                return r;
            }
    		for (int i=0;i<list.size();i++) {
    			if (i != si) {
    				dao = list.get(i);
    	    		r = dao.isHashFieldExist(key, field,index);
    	    		if (r) {
    	                break;
    	            }
    			}
    		}
    		
    		/*final Iterator<AbstractRedisDao> iterator = caches.iterator();
    		while (iterator.hasNext()) {
    			r = iterator.next().isHashFieldExist(key, field,index);
    			if (r) break;
    		}*/
    	}
		
		return r;
	}

	@Override
	public long getSetSize(String key, int index) {
		long r=0;
		if (CacheClusterConfig.ALG_POLLING.equals(this.clusterAlg)) {
			AbstractRedisDao dao = this.alg.getServerObject();
			r = dao.getSetSize(key,index);
    		if (log.isDebugEnabled()) {
    			log.debug("clusterAlg==" + this.clusterAlg + " Reids client " + dao.getName());
    		}	
    		
    	} else if (CacheClusterConfig.ALG_HASH.equals(this.clusterAlg)) {
    		AbstractRedisDao dao = this.alg.getServerObject(key);
    		r = dao.getSetSize(key,index);
    		if (log.isDebugEnabled()) {
    			log.debug("clusterAlg==" + this.clusterAlg + " Reids client " + dao.getName());
    		}	
    	} else {
    		List<AbstractRedisDao> list = this.alg.getAllServerObject();
    		int si = this.alg.getServerIndex();
    		AbstractRedisDao dao = list.get(si);
    		r = dao.getSetSize(key, index);
    		if ( r > 0) {
                return r;
            }
    		for (int i=0;i<list.size();i++) {
    			if (i != si) {
    				dao = list.get(i);
    	    		r = dao.getSetSize(key, index);
    	    		if (r > 0) {
    	                break;
    	            }
    			}
    		}
    		
    		/*final Iterator<AbstractRedisDao> iterator = caches.iterator();
    		while (iterator.hasNext()) {
    			r = iterator.next().getSetSize(key,index);
    			if (r > 0) break;
    		}*/
    	}
		
		return r;
	}

	@Override
	public Long publish(final String key, final String message) {
		Long ret = 0L;
		final Iterator<AbstractRedisDao> iterator = caches.iterator();
		while (iterator.hasNext()) {
			AbstractRedisDao dao = iterator.next();
			if (dao.isSlave())
				continue;
			ret = dao.publish(key,message);
			if (this.isAsyn)
				break;
		}
		if (iterator.hasNext()) {
			Runnable run = new Runnable() {
				@Override
				public void run() {
					while (iterator.hasNext()) {
						AbstractRedisDao dao = iterator.next();
						if (dao.isSlave()) continue;
							dao.publish(key,message);
					}
				}
			};
			this.invoke.exec(run);
		}
		return ret;
	}

	@Override
	public void subscribe(final JedisPubSub jedisPubSub, final String key) {
		final  AbstractRedisDao dao ;
		if (CacheClusterConfig.ALG_HASH.equals(this.clusterAlg)) {
    		dao = this.alg.getServerObject(key);
    		if (log.isDebugEnabled()) {
    			log.debug("clusterAlg==" + this.clusterAlg + " Reids client " + dao.getName());
    		}	
    	} else {
    			dao = this.alg.getServerObject();
        		if (log.isDebugEnabled()) {
        			log.debug("clusterAlg==" + this.clusterAlg + " Reids client " + dao.getName());
        		}	
    	}
		
		Runnable run = new Runnable() {
			@Override
			public void run() {
				dao.subscribe(jedisPubSub, key);

			}
		};
		this.invoke.exec(run);
		
		
	}

	@Override
	public Map<String, String> hgetAll(String key, int index) {
		 Map<String, String> r=null;
		if (CacheClusterConfig.ALG_POLLING.equals(this.clusterAlg)) {
			AbstractRedisDao dao = this.alg.getServerObject();
			r = dao.hgetAll(key,index);
    		if (log.isDebugEnabled()) {
    			log.debug("clusterAlg==" + this.clusterAlg + " Reids client " + dao.getName());
    		}	
    		
    	} else if (CacheClusterConfig.ALG_HASH.equals(this.clusterAlg)) {
    		AbstractRedisDao dao = this.alg.getServerObject(key);
    		r = dao.hgetAll(key,index);
    		if (log.isDebugEnabled()) {
    			log.debug("clusterAlg==" + this.clusterAlg + " Reids client " + dao.getName());
    		}	
    	} else {
    		List<AbstractRedisDao> list = this.alg.getAllServerObject();
    		int si = this.alg.getServerIndex();
    		AbstractRedisDao dao = list.get(si);
    		r = dao.hgetAll(key,index);
    		
    		if ( r != null) {
                return r;
            }
    		
    		for (int i=0;i<list.size();i++) {
    			if (i != si) {
    				dao = list.get(i);
    	    		r = dao.hgetAll(key,index);
    	    		if (r != null) {
    	                break;
    	            }
    			}
    		}
    		
    		/*final Iterator<AbstractRedisDao> iterator = caches.iterator();
    		while (iterator.hasNext()) {
    			r = iterator.next().hgetAll(key,index);
    			if (r != null) break;
    		}*/
    	}
		
		return r;
	}

	@Override
	public Map<String, Object> getHashAllObject(String key, int index) {
		 Map<String, Object> r=null;
			if (CacheClusterConfig.ALG_POLLING.equals(this.clusterAlg)) {
				AbstractRedisDao dao = this.alg.getServerObject();
				r = dao.getHashAllObject(key,index);
	    		if (log.isDebugEnabled()) {
	    			log.debug("clusterAlg==" + this.clusterAlg + " Reids client " + dao.getName());
	    		}	
	    		
	    	} else if (CacheClusterConfig.ALG_HASH.equals(this.clusterAlg)) {
	    		AbstractRedisDao dao = this.alg.getServerObject(key);
	    		r = dao.getHashAllObject(key,index);
	    		if (log.isDebugEnabled()) {
	    			log.debug("clusterAlg==" + this.clusterAlg + " Reids client " + dao.getName());
	    		}	
	    	} else {
	    		List<AbstractRedisDao> list = this.alg.getAllServerObject();
	    		int si = this.alg.getServerIndex();
	    		AbstractRedisDao dao = list.get(si);
	    		r = dao.getHashAllObject(key, index);
	    		if ( r != null) {
	                return r;
	            }
	    		
	    		for (int i=0;i<list.size();i++) {
	    			if (i != si) {
	    				dao = list.get(i);
	    	    		r = dao.getHashAllObject(key, index);
	    	    		if (r != null) {
	    	                break;
	    	            }
	    			}
	    		}
	    		
	    		/*final Iterator<AbstractRedisDao> iterator = caches.iterator();
	    		while (iterator.hasNext()) {
	    			r = iterator.next().getHashAllObject(key,index);
	    			if (r != null) break;
	    		}*/
	    	}
			
			return r;
	}

	@Override
	public String getName() {
		return this.name;
	}

	@Override
	public int getWeight() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean pingCheck() {
		if (this.failover == false) return true;
		List<AbstractRedisDao> tmp = new ArrayList<AbstractRedisDao> ();
		
		for (AbstractRedisDao ch: caches) {
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
				AbstractRedisDao dao = (AbstractRedisDao)ch;
				if (ping(dao)) {
					tmp.add(dao);
				} else {
					int trycn=0;
					while (trycn < PING_CN) {
						if (ping(dao)) {
							tmp.add(dao);
							break;
						}
						try {
							Thread.sleep(100);
						} catch (InterruptedException e) {
							
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
	
	public void init () {
    	if (CacheClusterConfig.ALG_HASH.equals(this.clusterAlg)) {
    		int weights [] = new int [caches.size()];
    		Map<String,AbstractRedisDao> map = new HashMap<String,AbstractRedisDao>();
    		for (int i=0;i<caches.size();i++) {
    			weights[i] = caches.get(i).getWeight();
    			map.put(caches.get(i).getName(), caches.get(i));
    		}
    		alg = new ConsistentHash<AbstractRedisDao>(weights, map);
		} else {

			int weights[] = new int[caches.size()];
			for (int i = 0; i < caches.size(); i++) {
				weights[i] = caches.get(i).getWeight();
			}
			alg = new WeightedRoundRobinScheduling<AbstractRedisDao>(weights, caches);

		}
	}
	
    private boolean compareCaches(List<AbstractRedisDao> tmp) {
    	if (tmp.size() != caches.size()) return false;
    	for(IMemCached ch:caches) {
    		boolean f = false;
    		for (AbstractRedisDao t:tmp) {
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
	
	private boolean ping (AbstractRedisDao ch) {
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
	public void setName(String name) {
		this.name = name;
		
	}

	@Override
	public void setWeight(int weight) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() {
	
		for(AbstractRedisDao c: caches) {
			c.close();
		}
		
	}

	@Override
	protected byte[] getBytes(byte[] key, int index) {
		byte [] r=null;
		if (CacheClusterConfig.ALG_POLLING.equals(this.clusterAlg)) {
			AbstractRedisDao dao = this.alg.getServerObject();
			r = dao.getBytes(key, index);
    		if (log.isDebugEnabled()) {
    			log.debug("clusterAlg==" + this.clusterAlg + " Reids client " + dao.getName());
    		}	
    		
    	} else if (CacheClusterConfig.ALG_HASH.equals(this.clusterAlg)) {
    		AbstractRedisDao dao = this.alg.getServerObject(key);
    		r = dao.getBytes(key, index);
    		if (log.isDebugEnabled()) {
    			log.debug("clusterAlg==" + this.clusterAlg + " Reids client " + dao.getName());
    		}	
    	} else {
    		List<AbstractRedisDao> list = this.alg.getAllServerObject();
    		int si = this.alg.getServerIndex();
    		AbstractRedisDao dao = list.get(si);
    		r = dao.getBytes(key, index);
    		if ( r != null) {
                return r;
            }
    		for (int i=0;i<list.size();i++) {
    			if (i != si) {
    				dao = list.get(i);
    	    		r = dao.getBytes(key, index);
    	    		if (r != null) {
    	                break;
    	            }
    			}
    		}
    		
    		/*final Iterator<AbstractRedisDao> iterator = caches.iterator();
    		while (iterator.hasNext()) {
    			r = iterator.next().getBytes(key, index);
    			if (r != null) break;
    		}*/
    	}
		
		return r;
	}

	@Override
	protected String setBytes(final byte[] key, final byte[] bytes, final int seconds,final int index) {
		String ret = null;
		final Iterator<AbstractRedisDao> iterator = caches.iterator();
		while (iterator.hasNext()) {
			AbstractRedisDao dao = iterator.next();
			if (dao.isSlave())
				continue;
			ret = dao.setBytes(key, bytes, seconds, index);
			if (this.isAsyn)
				break;
		}
		if (iterator.hasNext()) {
			Runnable run = new Runnable() {
				@Override
				public void run() {
					while (iterator.hasNext()) {
						AbstractRedisDao dao = iterator.next();
						if (dao.isSlave()) continue;
						dao.setBytes(key, bytes, seconds, index);
					}
				}
			};
			this.invoke.exec(run);
		}
		return ret;
	}

	@Override
	protected long del(String key, int index) {
		long ret = 0;
		final Iterator<AbstractRedisDao> iterator = caches.iterator();
		while (iterator.hasNext()) {
			AbstractRedisDao dao = iterator.next();
			if (dao.isSlave())
				continue;
			ret = dao.del(key,index);
		}
		
		return ret;
	}

	@Override
	protected boolean setBytesByBatch(final List<String> keyList,final List<byte[]> valueList, final int liveSeconds,final int index) {
		boolean ret = false;
		final Iterator<AbstractRedisDao> iterator = caches.iterator();
		while (iterator.hasNext()) {
			AbstractRedisDao dao = iterator.next();
			if (dao.isSlave())
				continue;
			ret = dao.setBytesByBatch(keyList, valueList, liveSeconds, index);
			if (this.isAsyn)
				break;
		}
		if (iterator.hasNext()) {
			Runnable run = new Runnable() {
				@Override
				public void run() {
					while (iterator.hasNext()) {
						AbstractRedisDao dao = iterator.next();
						if (dao.isSlave()) continue;
						dao.setBytesByBatch(keyList, valueList, liveSeconds, index);
					}
				}
			};
			this.invoke.exec(run);
		}
		return ret;
	}

	@Override
	protected List<Object> getBytesByBatch(List<String> keyList, int index) {
		List<Object> r=null;
		if (CacheClusterConfig.ALG_POLLING.equals(this.clusterAlg)) {
			AbstractRedisDao dao = this.alg.getServerObject();
			r = dao.getBytesByBatch(keyList, index);
    		if (log.isDebugEnabled()) {
    			log.debug("clusterAlg==" + this.clusterAlg + " Reids client " + dao.getName());
    		}	
    		
    	} else if (CacheClusterConfig.ALG_HASH.equals(this.clusterAlg)) {
    		AbstractRedisDao dao = this.alg.getServerObject(keyList.get(0));
    		r = dao.getBytesByBatch(keyList, index);
    		if (log.isDebugEnabled()) {
    			log.debug("clusterAlg==" + this.clusterAlg + " Reids client " + dao.getName());
    		}	
    	} else {
    		List<AbstractRedisDao> list = this.alg.getAllServerObject();
    		int si = this.alg.getServerIndex();
    		AbstractRedisDao dao = list.get(si);
    		r = dao.getBytesByBatch(keyList, index);
    		if ( r != null ) {
                return r;
            }
    		for (int i=0;i<list.size();i++) {
    			if (i != si) {
    				dao = list.get(i);
    	    		r = dao.getBytesByBatch(keyList, index);
    	    		if (r != null) {
    	                break;
    	            }
    			}
    		}
    		
    		/*final Iterator<AbstractRedisDao> iterator = caches.iterator();
    		while (iterator.hasNext()) {
    			r = iterator.next().getBytesByBatch(keyList, index);
    			if (r != null) break;
    		}*/
    	}
		
		return r;
	}

	@Override
	protected long getKeyExpiredTime(byte[] key, int index) {
		long r=0;
		if (CacheClusterConfig.ALG_POLLING.equals(this.clusterAlg)) {
			AbstractRedisDao dao = this.alg.getServerObject();
			r = dao.getKeyExpiredTime(key, index);
    		if (log.isDebugEnabled()) {
    			log.debug("clusterAlg==" + this.clusterAlg + " Reids client " + dao.getName());
    		}	
    		
    	} else if (CacheClusterConfig.ALG_HASH.equals(this.clusterAlg)) {
    		AbstractRedisDao dao = this.alg.getServerObject(key);
    		r = dao.getKeyExpiredTime(key, index);
    		if (log.isDebugEnabled()) {
    			log.debug("clusterAlg==" + this.clusterAlg + " Reids client " + dao.getName());
    		}	
    	} else {
    		List<AbstractRedisDao> list = this.alg.getAllServerObject();
    		int si = this.alg.getServerIndex();
    		AbstractRedisDao dao = list.get(si);
    		r = dao.getKeyExpiredTime(key, index);
    		if ( r>0) {
                return r;
            }
    		for (int i=0;i<list.size();i++) {
    			if (i != si) {
    				dao = list.get(i);
    	    		r = dao.getKeyExpiredTime(key, index);
    	    		if (r>0) {
    	                break;
    	            }
    			}
    		}
    		
    		/*final Iterator<AbstractRedisDao> iterator = caches.iterator();
    		while (iterator.hasNext()) {
    			r = iterator.next().getKeyExpiredTime(key, index);
    			if (r > 0) break;
    		}*/
    	}
		
		return r;
	}

	@Override
	protected boolean setHashBytes(final byte[] key,final byte[] field, final byte[] value,final int seconds, final int index) {
		boolean ret = false;
		final Iterator<AbstractRedisDao> iterator = caches.iterator();
		while (iterator.hasNext()) {
			AbstractRedisDao dao = iterator.next();
			if (dao.isSlave())
				continue;
			ret = dao.setHashBytes(key, field, value, seconds,index);
			if (this.isAsyn)
				break;
		}
		if (iterator.hasNext()) {
			Runnable run = new Runnable() {
				@Override
				public void run() {
					while (iterator.hasNext()) {
						AbstractRedisDao dao = iterator.next();
						if (dao.isSlave()) continue;
						dao.setHashBytes(key, field, value, seconds,index);
					}
				}
			};
			this.invoke.exec(run);
		}
		return ret;
	}

	@Override
	protected byte[] getHashBytes(byte[] key, byte[] fieldArray, int index) {
		byte [] r=null;
		if (CacheClusterConfig.ALG_POLLING.equals(this.clusterAlg)) {
			AbstractRedisDao dao = this.alg.getServerObject();
			r = dao.getHashBytes(key,fieldArray, index);
    		if (log.isDebugEnabled()) {
    			log.debug("clusterAlg==" + this.clusterAlg + " Reids client " + dao.getName());
    		}	
    		
    	} else if (CacheClusterConfig.ALG_HASH.equals(this.clusterAlg)) {
    		AbstractRedisDao dao = this.alg.getServerObject(key);
    		r = dao.getHashBytes(key,fieldArray, index);
    		if (log.isDebugEnabled()) {
    			log.debug("clusterAlg==" + this.clusterAlg + " Reids client " + dao.getName());
    		}	
    	} else {
    		List<AbstractRedisDao> list = this.alg.getAllServerObject();
    		int si = this.alg.getServerIndex();
    		AbstractRedisDao dao = list.get(si);
    		r = dao.getHashBytes(key,fieldArray, index);
    		if ( r != null) {
                return r;
            }
    		for (int i=0;i<list.size();i++) {
    			if (i != si) {
    				dao = list.get(i);
    	    		r = dao.getHashBytes(key,fieldArray, index);
    	    		if (r != null) {
    	                break;
    	            }
    			}
    		}
    		
    		/*final Iterator<AbstractRedisDao> iterator = caches.iterator();
    		while (iterator.hasNext()) {
    			r = iterator.next().getHashBytes(key, fieldArray,index);
    			if (r != null) break;
    		}*/
    	}
		
		return r;
	}

	@Override
	protected List<byte[]> getHashBytesByBatch(byte[] key, byte[][] fieldArray,int index) {
		List<byte[]> r=null;
		if (CacheClusterConfig.ALG_POLLING.equals(this.clusterAlg)) {
			AbstractRedisDao dao = this.alg.getServerObject();
			r = dao.getHashBytesByBatch(key,fieldArray, index);
    		if (log.isDebugEnabled()) {
    			log.debug("clusterAlg==" + this.clusterAlg + " Reids client " + dao.getName());
    		}	
    		
    	} else if (CacheClusterConfig.ALG_HASH.equals(this.clusterAlg)) {
    		AbstractRedisDao dao = this.alg.getServerObject(key);
    		r = dao.getHashBytesByBatch(key,fieldArray, index);
    		if (log.isDebugEnabled()) {
    			log.debug("clusterAlg==" + this.clusterAlg + " Reids client " + dao.getName());
    		}	
    	} else {
    		
    		List<AbstractRedisDao> list = this.alg.getAllServerObject();
    		int si = this.alg.getServerIndex();
    		AbstractRedisDao dao = list.get(si);
    		r = dao.getHashBytesByBatch(key, fieldArray,index);
    		
    		if ( r != null) {
                return r;
            }
    		
    		for (int i=0;i<list.size();i++) {
    			if (i != si) {
    				dao = list.get(i);
    	    		r = dao.getHashBytesByBatch(key, fieldArray,index);
    	    		if (r != null) {
    	                break;
    	            }
    			}
    		}
    		
    		/*final Iterator<AbstractRedisDao> iterator = caches.iterator();
    		while (iterator.hasNext()) {
    			r = iterator.next().getHashBytesByBatch(key, fieldArray,index);
    			if (r != null) break;
    		}*/
    	}
		
		return r;
	}

	@Override
	protected boolean setHashBytesByBatch(final byte[] key, final Map<byte[], byte[]> map,final int seconds, final int index) {
		boolean ret = false;
		final Iterator<AbstractRedisDao> iterator = caches.iterator();
		while (iterator.hasNext()) {
			AbstractRedisDao dao = iterator.next();
			if (dao.isSlave())
				continue;
			ret = dao.setHashBytesByBatch(key, map, seconds, index);
			if (this.isAsyn)
				break;
		}
		if (iterator.hasNext()) {
			Runnable run = new Runnable() {
				@Override
				public void run() {
					while (iterator.hasNext()) {
						AbstractRedisDao dao = iterator.next();
						if (dao.isSlave()) continue;
						dao.setHashBytesByBatch(key, map, seconds, index);
					}
				}
			};
			this.invoke.exec(run);
		}
		return ret;
	}

	@Override
	protected boolean delHashBytes(final byte[] key, final byte[] field, final int index) {
		boolean ret = false;
		final Iterator<AbstractRedisDao> iterator = caches.iterator();
		while (iterator.hasNext()) {
			AbstractRedisDao dao = iterator.next();
			if (dao.isSlave())
				continue;
			ret = dao.delHashBytes(key, field, index);
			//break;
		}
		
		/*if (iterator.hasNext()) {
			Runnable run = new Runnable() {
				@Override
				public void run() {
					while (iterator.hasNext()) {
						AbstractRedisDao dao = iterator.next();
						if (dao.isSlave()) continue;
						dao.delHashBytes(key, field, index);
					}
				}
			};
			this.invoke.exec(run);
		}*/
		return ret;
	}

	@Override
	protected boolean delHashBytesByBatch(byte[] key, byte[][] fieldArray,int index) {
		boolean ret = false;
		final Iterator<AbstractRedisDao> iterator = caches.iterator();
		while (iterator.hasNext()) {
			AbstractRedisDao dao = iterator.next();
			if (dao.isSlave())
				continue;
			ret = dao.delHashBytesByBatch(key, fieldArray, index);
			//break;
		}
		/*if (iterator.hasNext()) {
			Runnable run = new Runnable() {
				@Override
				public void run() {
					while (iterator.hasNext()) {
						AbstractRedisDao dao = iterator.next();
						if (dao.isSlave()) continue;
						dao.delHashBytesByBatch(key, fieldArray, index);
					}
				}
			};
			this.invoke.exec(run);
		}*/
		
		return ret;
	}

	@Override
	protected boolean addSetBytes(final byte[] key, final byte[] member, final int seconds,final int index) {
		boolean ret = false;
		final Iterator<AbstractRedisDao> iterator = caches.iterator();
		while (iterator.hasNext()) {
			AbstractRedisDao dao = iterator.next();
			if (dao.isSlave())
				continue;
			ret = dao.addSetBytes (key, member, seconds, index);
			if (this.isAsyn)
				break;
		}
		if (iterator.hasNext()) {
			Runnable run = new Runnable() {
				@Override
				public void run() {
					while (iterator.hasNext()) {
						AbstractRedisDao dao = iterator.next();
						if (dao.isSlave()) continue;
						dao.addSetBytes (key, member, seconds, index);
					}
				}
			};
			this.invoke.exec(run);
		}
		return ret;
	}

	@Override
	protected boolean addSetBytesByBatch(final byte[] key, final byte[][] byteArray,final int seconds, final int index) {
		boolean ret = false;
		final Iterator<AbstractRedisDao> iterator = caches.iterator();
		while (iterator.hasNext()) {
			AbstractRedisDao dao = iterator.next();
			if (dao.isSlave())
				continue;
			ret = dao.addSetBytesByBatch(key, byteArray, seconds, index);
			if (this.isAsyn)
				break;
		}
		if (iterator.hasNext()) {
			Runnable run = new Runnable() {
				@Override
				public void run() {
					while (iterator.hasNext()) {
						AbstractRedisDao dao = iterator.next();
						if (dao.isSlave()) continue;
						dao.addSetBytesByBatch(key, byteArray, seconds, index);
					}
				}
			};
			this.invoke.exec(run);
		}
		return ret;
	}

	@Override
	protected boolean isSetByteExists(final byte[] key,final byte[] member, final int index) {
		boolean r=false;
		if (CacheClusterConfig.ALG_POLLING.equals(this.clusterAlg)) {
			AbstractRedisDao dao = this.alg.getServerObject();
			r = dao.isSetByteExists(key,member, index);
    		if (log.isDebugEnabled()) {
    			log.debug("clusterAlg==" + this.clusterAlg + " Reids client " + dao.getName());
    		}	
    		
    	} else if (CacheClusterConfig.ALG_HASH.equals(this.clusterAlg)) {
    		AbstractRedisDao dao = this.alg.getServerObject(key);
    		r = dao.isSetByteExists(key,member, index);
    		if (log.isDebugEnabled()) {
    			log.debug("clusterAlg==" + this.clusterAlg + " Reids client " + dao.getName());
    		}	
    	} else {
    		
    		List<AbstractRedisDao> list = this.alg.getAllServerObject();
    		int si = this.alg.getServerIndex();
    		AbstractRedisDao dao = list.get(si);
    		r = dao.isSetByteExists(key,member, index);
    		if ( r ) {
                return r;
            }
    		
    		for (int i=0;i<list.size();i++) {
    			if (i != si) {
    				dao = list.get(i);
    				r = dao.isSetByteExists(key,member, index);
    	    		if ( r ) {
    	                break;
    	            }
    			}
    		}
    		/*
    		final Iterator<AbstractRedisDao> iterator = caches.iterator();
    		while (iterator.hasNext()) {
    			r = iterator.next().isSetByteExists(key,member, index);
    			if (r ) break;
    		}*/
    	}
		
		return r;
	}

	@Override
	protected boolean delSetByte(final byte[] key, final byte[] member, final int index) {
		boolean ret = false;
		final Iterator<AbstractRedisDao> iterator = caches.iterator();
		while (iterator.hasNext()) {
			AbstractRedisDao dao = iterator.next();
			if (dao.isSlave())
				continue;
			ret = dao.delSetByte(key, member, index);
			//break;
		}
		
		/*if (iterator.hasNext()) {
			Runnable run = new Runnable() {
				@Override
				public void run() {
					while (iterator.hasNext()) {
						AbstractRedisDao dao = iterator.next();
						if (dao.isSlave()) continue;
						dao.delSetByte(key, member, index);
					}
				}
			};
			this.invoke.exec(run);
		}*/
		return ret;
	}

	@Override
	protected boolean delSetBytesByBatch(final byte[] key, final byte[][] byteArray,final int index) {
		boolean ret = false;
		final Iterator<AbstractRedisDao> iterator = caches.iterator();
		while (iterator.hasNext()) {
			AbstractRedisDao dao = iterator.next();
			if (dao.isSlave())
				continue;
			ret = dao.delSetBytesByBatch(key, byteArray, index);
			//break;
		}
		/*if (iterator.hasNext()) {
			Runnable run = new Runnable() {
				@Override
				public void run() {
					while (iterator.hasNext()) {
						AbstractRedisDao dao = iterator.next();
						if (dao.isSlave()) continue;
						dao.delSetBytesByBatch(key, byteArray, index);
					}
				}
			};
			this.invoke.exec(run);
		}*/
		return ret;
	}

	@Override
	protected Set<byte[]> getAllSetBytes(byte[] key, int index) {
		Set<byte[]> r=null;
		if (CacheClusterConfig.ALG_POLLING.equals(this.clusterAlg)) {
			AbstractRedisDao dao = this.alg.getServerObject();
			r = dao.getAllSetBytes(key, index);
    		if (log.isDebugEnabled()) {
    			log.debug("clusterAlg==" + this.clusterAlg + " Reids client " + dao.getName());
    		}	
    		
    	} else if (CacheClusterConfig.ALG_HASH.equals(this.clusterAlg)) {
    		AbstractRedisDao dao = this.alg.getServerObject(key);
    		r = dao.getAllSetBytes(key, index);
    		if (log.isDebugEnabled()) {
    			log.debug("clusterAlg==" + this.clusterAlg + " Reids client " + dao.getName());
    		}	
    	} else {
    		List<AbstractRedisDao> list = this.alg.getAllServerObject();
    		int si = this.alg.getServerIndex();
    		AbstractRedisDao dao = list.get(si);
    		r = dao.getAllSetBytes(key,index);
    		
    		if ( r != null) {
                return r;
            }
    		
    		for (int i=0;i<list.size();i++) {
    			if (i != si) {
    				dao = list.get(i);
    	    		r = dao.getAllSetBytes(key,index);
    	    		if (r != null) {
    	                break;
    	            }
    			}
    		}
    		
    		/*final Iterator<AbstractRedisDao> iterator = caches.iterator();
    		while (iterator.hasNext()) {
    			r = iterator.next().getAllSetBytes(key,index);
    			if (r != null) break;
    		}*/
    	}
		
		return r;
	}

	@Override
	protected boolean addSortedSetByte(final byte[] key, final byte[] member,final double score,final int seconds,final int index) {
		boolean ret = false;
		final Iterator<AbstractRedisDao> iterator = caches.iterator();
		while (iterator.hasNext()) {
			AbstractRedisDao dao = iterator.next();
			if (dao.isSlave())
				continue;
			ret = dao.addSortedSetByte(key, member, score,seconds, index);
			if (this.isAsyn)
				break;
		}
		if (iterator.hasNext()) {
			Runnable run = new Runnable() {
				@Override
				public void run() {
					while (iterator.hasNext()) {
						AbstractRedisDao dao = iterator.next();
						if (dao.isSlave()) continue;
						 dao.addSortedSetByte(key, member, score,seconds, index);
					}
				}
			};
			this.invoke.exec(run);
		}
		return ret;
	}

	@Override
	protected boolean addSortedSetByteByBatch(final byte[] key, final Map<byte[], Double> memberBytes, final int seconds, final int index) {
		boolean ret = false;
		final Iterator<AbstractRedisDao> iterator = caches.iterator();
		while (iterator.hasNext()) {
			AbstractRedisDao dao = iterator.next();
			if (dao.isSlave())
				continue;
			ret = dao.addSortedSetByteByBatch(key, memberBytes, seconds, index);
			if (this.isAsyn)
				break;
		}
		if (iterator.hasNext()) {
			Runnable run = new Runnable() {
				@Override
				public void run() {
					while (iterator.hasNext()) {
						AbstractRedisDao dao = iterator.next();
						if (dao.isSlave()) continue;
						dao.addSortedSetByteByBatch(key, memberBytes, seconds, index);
					}
				}
			};
			this.invoke.exec(run);
		}
		return ret;
	}

	@Override
	public long getSortedSetSize(String key, int index) {
		long r=0;
		if (CacheClusterConfig.ALG_POLLING.equals(this.clusterAlg)) {
			AbstractRedisDao dao = this.alg.getServerObject();
			r = dao.getSortedSetSize(key, index);
    		if (log.isDebugEnabled()) {
    			log.debug("clusterAlg==" + this.clusterAlg + " Reids client " + dao.getName());
    		}	
    		
    	} else if (CacheClusterConfig.ALG_HASH.equals(this.clusterAlg)) {
    		AbstractRedisDao dao = this.alg.getServerObject(key);
    		r = dao.getSortedSetSize(key, index);
    		if (log.isDebugEnabled()) {
    			log.debug("clusterAlg==" + this.clusterAlg + " Reids client " + dao.getName());
    		}	
    	} else {
    		List<AbstractRedisDao> list = this.alg.getAllServerObject();
    		int si = this.alg.getServerIndex();
    		AbstractRedisDao dao = list.get(si);
    		r = dao.getSortedSetSize(key,index);
    		if ( r > 0) {
                return r;
            }
    		
    		for (int i=0;i<list.size();i++) {
    			if (i != si) {
    				dao = list.get(i);
    	    		r = dao.getSortedSetSize(key,index);
    	    		if (r > 0) {
    	                break;
    	            }
    			}
    		}
    		
    		/*final Iterator<AbstractRedisDao> iterator = caches.iterator();
    		while (iterator.hasNext()) {
    			r = iterator.next().getSortedSetSize(key,index);
    			if (r > 0) break;
    		}*/
    	}
		
		return r;
	}

	@Override
	protected double incrSortedSetByteScore(final byte[] key, final byte[] member,final double score,final int index) {
		double ret = 0;
		final Iterator<AbstractRedisDao> iterator = caches.iterator();
		while (iterator.hasNext()) {
			AbstractRedisDao dao = iterator.next();
			if (dao.isSlave())
				continue;
			ret = dao.incrSortedSetByteScore(key, member, score, index);
			if (this.isAsyn)
				break;
		}
		if (iterator.hasNext()) {
			Runnable run = new Runnable() {
				@Override
				public void run() {
					while (iterator.hasNext()) {
						AbstractRedisDao dao = iterator.next();
						if (dao.isSlave()) continue;
						dao.incrSortedSetByteScore(key, member, score, index);
					}
				}
			};
			this.invoke.exec(run);
		}
		return ret;
	}

	@Override
	protected Set<byte[]> getSortedSetByteRange(byte[] key, long start,long end, int index) {
		Set<byte[]> r=null;
		if (CacheClusterConfig.ALG_POLLING.equals(this.clusterAlg)) {
			AbstractRedisDao dao = this.alg.getServerObject();
			r = dao.getSortedSetByteRange(key,start, end,index);
    		if (log.isDebugEnabled()) {
    			log.debug("clusterAlg==" + this.clusterAlg + " Reids client " + dao.getName());
    		}	
    		
    	} else if (CacheClusterConfig.ALG_HASH.equals(this.clusterAlg)) {
    		AbstractRedisDao dao = this.alg.getServerObject(key);
    		r = dao.getSortedSetByteRange(key,start, end,index);
    		if (log.isDebugEnabled()) {
    			log.debug("clusterAlg==" + this.clusterAlg + " Reids client " + dao.getName());
    		}	
    	} else {
    		
    		List<AbstractRedisDao> list = this.alg.getAllServerObject();
    		int si = this.alg.getServerIndex();
    		AbstractRedisDao dao = list.get(si);
    		r = dao.getSortedSetByteRange(key,start, end,index);
    		
    		if ( r != null) {
                return r;
            }
    		
    		for (int i=0;i<list.size();i++) {
    			if (i != si) {
    				dao = list.get(i);
    	    		r = dao.getSortedSetByteRange(key,start, end,index);
    	    		if (r != null) {
    	                break;
    	            }
    			}
    		}
    		
    		/*
    		final Iterator<AbstractRedisDao> iterator = caches.iterator();
    		while (iterator.hasNext()) {
    			r = iterator.next().getSortedSetByteRange(key,start, end,index);
    			if (r != null) break;
    		}*/
    	}
		
		return r;
	}

	@Override
	protected Set<byte[]> getSortedSetByteRevRange(byte[] key, long start,long end, int index) {
		Set<byte[]> r=null;
		if (CacheClusterConfig.ALG_POLLING.equals(this.clusterAlg)) {
			AbstractRedisDao dao = this.alg.getServerObject();
			r = dao.getSortedSetByteRevRange(key,start, end,index);
    		if (log.isDebugEnabled()) {
    			log.debug("clusterAlg==" + this.clusterAlg + " Reids client " + dao.getName());
    		}	
    		
    	} else if (CacheClusterConfig.ALG_HASH.equals(this.clusterAlg)) {
    		AbstractRedisDao dao = this.alg.getServerObject(key);
    		r = dao.getSortedSetByteRevRange(key,start,end, index);
    		if (log.isDebugEnabled()) {
    			log.debug("clusterAlg==" + this.clusterAlg + " Reids client " + dao.getName());
    		}	
    	} else {
    		
    		List<AbstractRedisDao> list = this.alg.getAllServerObject();
    		int si = this.alg.getServerIndex();
    		AbstractRedisDao dao = list.get(si);
    		r = dao.getSortedSetByteRevRange(key, start,end,index);
    		
    		if ( r != null) {
                return r;
            }
    		
    		for (int i=0;i<list.size();i++) {
    			if (i != si) {
    				dao = list.get(i);
    	    		r = dao.getSortedSetByteRevRange(key, start,end,index);
    	    		if (r != null) {
    	                break;
    	            }
    			}
    		}
    		
    	/*	
    		final Iterator<AbstractRedisDao> iterator = caches.iterator();
    		while (iterator.hasNext()) {
    			r = iterator.next().getSortedSetByteRevRange(key, start,end,index);
    			if (r != null) break;
    		}*/
    	}
		
		return r;
	}

	@Override
	protected boolean delSortedSetByte(final byte[] key, final byte[] member,final int index) {
		boolean ret = false;
		final Iterator<AbstractRedisDao> iterator = caches.iterator();
		while (iterator.hasNext()) {
			AbstractRedisDao dao = iterator.next();
			if (dao.isSlave())
				continue;
			ret = dao.delSortedSetByte(key, member, index);
			//break;
		}
		/*if (iterator.hasNext()) {
			Runnable run = new Runnable() {
				@Override
				public void run() {
					while (iterator.hasNext()) {
						AbstractRedisDao dao = iterator.next();
						if (dao.isSlave()) continue;
						 dao.delSortedSetByte(key, member, index);
					}
				}
			};
			this.invoke.exec(run);
		}*/
		return ret;
	}

	@Override
	protected boolean delSortedSetBytesByBatch(final byte[] key, final byte[][] byteArray,final int index) {
		boolean ret = false;
		final Iterator<AbstractRedisDao> iterator = caches.iterator();
		while (iterator.hasNext()) {
			AbstractRedisDao dao = iterator.next();
			if (dao.isSlave())
				continue;
			ret = dao.delSortedSetBytesByBatch(key, byteArray, index);
			//break;
		}
		/*if (iterator.hasNext()) {
			Runnable run = new Runnable() {
				@Override
				public void run() {
					while (iterator.hasNext()) {
						AbstractRedisDao dao = iterator.next();
						if (dao.isSlave()) continue;
						dao.delSortedSetBytesByBatch(key, byteArray, index);
					}
				}
			};
			this.invoke.exec(run);
		}*/
		
		return ret;
	}

	@Override
	public ICachedConfigManager getCacheManager() {
		
		return this.cacheManager;
	}

	@Override
	public boolean keyExists(String key) {
		boolean r=false;
		if (CacheClusterConfig.ALG_POLLING.equals(this.clusterAlg)) {
			AbstractRedisDao dao = this.alg.getServerObject();
			r = dao.keyExists(key);
    		if (log.isDebugEnabled()) {
    			log.debug("clusterAlg==" + this.clusterAlg + " Reids client " + dao.getName());
    		}	
    		
    	} else if (CacheClusterConfig.ALG_HASH.equals(this.clusterAlg)) {
    		AbstractRedisDao dao = this.alg.getServerObject(key);
    		r = dao.keyExists(key);
    		if (log.isDebugEnabled()) {
    			log.debug("clusterAlg==" + this.clusterAlg + " Reids client " + dao.getName());
    		}	
    	} else {
    		List<AbstractRedisDao> list = this.alg.getAllServerObject();
    		int si = this.alg.getServerIndex();
    		AbstractRedisDao dao = list.get(si);
    		r = dao.keyExists(key);
    		if ( r ) {
                return r;
            }
    		
    		for (int i=0;i<list.size();i++) {
    			if (i != si) {
    				dao = list.get(i);
    				r = dao.keyExists(key);
    	    		if ( r ) {
    	                break;
    	            }
    			}
    		}
    		/*
    		final Iterator<AbstractRedisDao> iterator = caches.iterator();
    		while (iterator.hasNext()) {
    			r = iterator.next().keyExists(key);
    			if (r) break;
    		}*/
    	}
		
		return r;
	}

	@Override
	public boolean delete(final String key) {
		boolean ret = false;
		final Iterator<AbstractRedisDao> iterator = caches.iterator();
		while (iterator.hasNext()) {
			AbstractRedisDao dao = iterator.next();
			if (dao.isSlave())
				continue;
			ret = dao.delete(key);
			//break;
		}
		/*if (iterator.hasNext()) {
			Runnable run = new Runnable() {
				@Override
				public void run() {
					while (iterator.hasNext()) {
						AbstractRedisDao dao = iterator.next();
						if (dao.isSlave()) continue;
						dao.delete(key);
					}
				}
			};
			this.invoke.exec(run);
		}*/
		
		return ret;
	}

	
	protected boolean setBytes(final byte[] key, final byte[] bytes, final int seconds, final int index, final int flag) {
		boolean ret = false;
		final Iterator<AbstractRedisDao> iterator = caches.iterator();
		while (iterator.hasNext()) {
			AbstractRedisDao dao = iterator.next();
			if (dao.isSlave())
				continue;
			ret = dao.setBytes(key, bytes, seconds, index, flag);
			if (this.isAsyn)
				break;
		}
		if (iterator.hasNext()) {
			Runnable run = new Runnable() {
				@Override
				public void run() {
					while (iterator.hasNext()) {
						AbstractRedisDao dao = iterator.next();
						if (dao.isSlave()) continue;
						dao.setBytes(key, bytes, seconds, index, flag);
					}
				}
			};
			this.invoke.exec(run);
		}
		return ret;
	}

	@Override
	public Object get(String key) {
		Object r=null;
		if (CacheClusterConfig.ALG_POLLING.equals(this.clusterAlg)) {
			AbstractRedisDao dao = this.alg.getServerObject();
			r = dao.get(key);
    		if (log.isDebugEnabled()) {
    			log.debug("clusterAlg==" + this.clusterAlg + " Reids client " + dao.getName());
    		}	
    		
    	} else if (CacheClusterConfig.ALG_HASH.equals(this.clusterAlg)) {
    		AbstractRedisDao dao = this.alg.getServerObject(key);
    		r = dao.get(key);
    		if (log.isDebugEnabled()) {
    			log.debug("clusterAlg==" + this.clusterAlg + " Reids client " + dao.getName());
    		}	
    	} else {
    		
    		List<AbstractRedisDao> list = this.alg.getAllServerObject();
    		int si = this.alg.getServerIndex();
    		AbstractRedisDao dao = list.get(si);
    		r = dao.get(key);
    		
    		if ( r != null) {
                return r;
            }
    		
    		for (int i=0;i<list.size();i++) {
    			if (i != si) {
    				dao = list.get(i);
    				r = dao.get(key);
    	    		if (r != null) {
    	                break;
    	            }
    			}
    		}
    		
    		
    		/*final Iterator<AbstractRedisDao> iterator = caches.iterator();
    		while (iterator.hasNext()) {
    			r = iterator.next().get(key);
    			if (r != null) break;
    		}*/
    	}
		
		return r;
	}

	@Override
	public Object[] getMultiArray(String[] keys) {
		Object r [] = null;
		if (CacheClusterConfig.ALG_POLLING.equals(this.clusterAlg)) {
			AbstractRedisDao dao = this.alg.getServerObject();
			r = dao.getMultiArray(keys);
    		if (log.isDebugEnabled()) {
    			log.debug("clusterAlg==" + this.clusterAlg + " Reids client " + dao.getName());
    		}	
    		
    	} else if (CacheClusterConfig.ALG_HASH.equals(this.clusterAlg)) {
    		AbstractRedisDao dao = this.alg.getServerObject(keys[0]);
    		r = dao.getMultiArray(keys);
    		if (log.isDebugEnabled()) {
    			log.debug("clusterAlg==" + this.clusterAlg + " Reids client " + dao.getName());
    		}	
    	} else {
    		List<AbstractRedisDao> list = this.alg.getAllServerObject();
    		int si = this.alg.getServerIndex();
    		AbstractRedisDao dao = list.get(si);
    		r = dao.getMultiArray(keys);
    		
    		if ( r != null) {
                return r;
            }
    		
    		for (int i=0;i<list.size();i++) {
    			if (i != si) {
    				dao = list.get(i);
    	    		r = dao.getMultiArray(keys);
    	    		if (r != null) {
    	                break;
    	            }
    			}
    		}
    		
    		/*final Iterator<AbstractRedisDao> iterator = caches.iterator();
    		while (iterator.hasNext()) {
    			r = iterator.next().getMultiArray(keys);
    			if (r != null) break;
    		}*/
    	}
		
		return r;
	}

	@Override
	public Map<String, Object> getMulti(String[] keys) {
		 Map<String, Object> r=null;
			if (CacheClusterConfig.ALG_POLLING.equals(this.clusterAlg)) {
				AbstractRedisDao dao = this.alg.getServerObject();
				r = dao.getMulti(keys);
	    		if (log.isDebugEnabled()) {
	    			log.debug("clusterAlg==" + this.clusterAlg + " Reids client " + dao.getName());
	    		}	
	    		
	    	} else if (CacheClusterConfig.ALG_HASH.equals(this.clusterAlg)) {
	    		AbstractRedisDao dao = this.alg.getServerObject(keys[0]);
	    		r = dao.getMulti(keys);
	    		if (log.isDebugEnabled()) {
	    			log.debug("clusterAlg==" + this.clusterAlg + " Reids client " + dao.getName());
	    		}	
	    	} else {
	    		
	    		List<AbstractRedisDao> list = this.alg.getAllServerObject();
	    		int si = this.alg.getServerIndex();
	    		AbstractRedisDao dao = list.get(si);
	    		r =dao.getMulti(keys);
	    		
	    		if ( r != null) {
	                return r;
	            }
	    		
	    		for (int i=0;i<list.size();i++) {
	    			if (i != si) {
	    				dao = list.get(i);
	    	    		r = dao.getMulti(keys);
	    	    		if (r != null) {
	    	                break;
	    	            }
	    			}
	    		}
	    		
	    		/*final Iterator<AbstractRedisDao> iterator = caches.iterator();
	    		while (iterator.hasNext()) {
	    			r = iterator.next().getMulti(keys);
	    			if (r != null) break;
	    		}*/
	    	}
			
			return r;
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

	@Override
	public List<IMemCached> copyCaches () {
		if (this.caches == null) return null;
		 ArrayList<IMemCached> tmp = new ArrayList<IMemCached> ();
		 for (IMemCached ch:caches) {
			 tmp.add(ch);
		 }
		return tmp;
	}
	
	public void setCacheManager(ICachedConfigManager cacheManager) {
		this.cacheManager = cacheManager;
	}
	
	@Override
	 public void setCaches(List<IMemCached> caches) {
		 if (caches == null || caches.size() <=0) throw new RuntimeException("setCaches caches is null");
		 ArrayList<AbstractRedisDao> tmp = new ArrayList<AbstractRedisDao> ();
		for (IMemCached ch : caches) {
			tmp.add((AbstractRedisDao) ch);
		}
		this.caches = tmp;
		caches.clear();
		this.init();
		if (badCaches != null && badCaches.size() > 0) {
			this.cacheManager.destory(badCaches.values());
			badCaches.clear();
		} else {
			badCaches = new HashMap<String, IMemCached>();
		}
	}

	public boolean isAsyn() {
		return isAsyn;
	}

	public void setAsyn(boolean isAsyn) {
		this.isAsyn = isAsyn;
	}

	@Override
	public long decr(final String key, final int index) {
		long ret = 0;
		final Iterator<AbstractRedisDao> iterator = caches.iterator();
		while (iterator.hasNext()) {
			AbstractRedisDao dao = iterator.next();
			if (dao.isSlave())
				continue;
			ret = dao.decr(key, index);
			if (this.isAsyn)
				break;
		}
		if (iterator.hasNext()) {
			Runnable run = new Runnable() {
				@Override
				public void run() {
					while (iterator.hasNext()) {
						AbstractRedisDao dao = iterator.next();
						if (dao.isSlave()) continue;
						    dao.decr(key, index);
					}
				}
			};
			this.invoke.exec(run);
		}
		return ret;
	}

	@Override
	public long incrBy(final String key, final long integer, final int index) {
		long ret = 0;
		final Iterator<AbstractRedisDao> iterator = caches.iterator();
		while (iterator.hasNext()) {
			AbstractRedisDao dao = iterator.next();
			if (dao.isSlave())
				continue;
			ret = dao.incrBy(key, integer,index);
			if (this.isAsyn)
				break;
		}
		if (iterator.hasNext()) {
			Runnable run = new Runnable() {
				@Override
				public void run() {
					while (iterator.hasNext()) {
						AbstractRedisDao dao = iterator.next();
						if (dao.isSlave()) continue;
						dao.incrBy(key, integer,index);
					}
				}
			};
			this.invoke.exec(run);
		}
		return ret;
	}

	@Override
	public long decrBy(final String key, final long integer,final int index) {
		// TODO Auto-generated method stub
		long ret = 0;
		final Iterator<AbstractRedisDao> iterator = caches.iterator();
		while (iterator.hasNext()) {
			AbstractRedisDao dao = iterator.next();
			if (dao.isSlave())
				continue;
			ret = dao.decrBy(key, integer,index);
			if (this.isAsyn)
				break;
		}
		if (iterator.hasNext()) {
			Runnable run = new Runnable() {
				@Override
				public void run() {
					while (iterator.hasNext()) {
						AbstractRedisDao dao = iterator.next();
						if (dao.isSlave()) continue;
						dao.decrBy(key, integer,index);
					}
				}
			};
			this.invoke.exec(run);
		}
		return ret;
	}

	@Override
	public boolean flushAll(String[] servers) {
		boolean r = false;
		for ( AbstractRedisDao dao: caches) {
			r = dao.flushAll(servers);
		}
		return r;
	}
}
