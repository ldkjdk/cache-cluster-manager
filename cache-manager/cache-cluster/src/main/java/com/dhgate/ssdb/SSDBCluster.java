package com.dhgate.ssdb;

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
import com.dhgate.memcache.ClusterProcessorManager;
import com.dhgate.memcache.ICacheCluster;
import com.dhgate.memcache.IMemCached;

public class SSDBCluster extends AbstractSSDBDao implements ICacheCluster{

	private static final Logger log          = LoggerFactory.getLogger(AbstractSSDBDao.class);
	 
    private String            name;
	private String clusterAlg;
	private boolean failover = true;

	private List<AbstractSSDBDao> caches;
	private Map<String, IMemCached> badCaches = null;
	
	private ICachedConfigManager cacheManager;
	private RoutingAlg<AbstractSSDBDao> alg;
	private boolean   isAsyn  = true;
	ClusterProcessorManager invoke = ClusterProcessorManager.newInstance();
	
	@Override
	public boolean del(String key) {
		boolean ret = false;
		final Iterator<AbstractSSDBDao> iterator = caches.iterator();
		while (iterator.hasNext()) {
			AbstractSSDBDao dao = iterator.next();
			if (dao.isSlave())
				continue;
			ret = dao.del(key);
		}
		
		return ret;
	}

	@Override
	public boolean isExist(String key) {
		boolean r = false;
		if (CacheClusterConfig.ALG_POLLING.equals(this.clusterAlg)) {
			AbstractSSDBDao dao = this.alg.getServerObject();
			r = dao.isExist(key);
    		if (log.isDebugEnabled()) {
    			log.debug("clusterAlg==" + this.clusterAlg + " SSDB client " + dao.getName());
    		}	
    		
    	} else if (CacheClusterConfig.ALG_HASH.equals(this.clusterAlg)) {
    		AbstractSSDBDao dao = this.alg.getServerObject(key);
    		r = dao.isExist(key);
    		if (log.isDebugEnabled()) {
    			log.debug("clusterAlg==" + this.clusterAlg + " SSDB client " + dao.getName());
    		}	
    	} else {
    		
    		List<AbstractSSDBDao> list = this.alg.getAllServerObject();
    		int si = this.alg.getServerIndex();
    		AbstractSSDBDao dao = list.get(si);
    		r = dao.isExist(key);
    		if (r ) {
                return r;
            }
    		
    		for (int i=0;i<list.size();i++) {
    			if (i != si) {
    				dao = list.get(i);
    	    		r = dao.isExist(key);
    	    		if (r) {
    	                break;
    	            }
    			}
    		}
    	}
		
		return r;
	}

	@Override
	public long incr(final String key, final long incr) {
		long ret = 0;
		final Iterator<AbstractSSDBDao> iterator = caches.iterator();
		while (iterator.hasNext()) {
			AbstractSSDBDao dao = iterator.next();
			if (dao.isSlave())
				continue;
			ret = dao.incr(key,incr);
			if (this.isAsyn)
				break;
		}
		if (iterator.hasNext()) {
			Runnable run = new Runnable() {
				@Override
				public void run() {
					while (iterator.hasNext()) {
						AbstractSSDBDao dao = iterator.next();
						if (dao.isSlave()) continue;
						dao.incr(key,incr);
					}
				}
			};
			this.invoke.exec(run);
		}
		return ret;
	}

	@Override
	public long decr(final String key, final long decr) {
		long ret = 0;
		final Iterator<AbstractSSDBDao> iterator = caches.iterator();
		while (iterator.hasNext()) {
			AbstractSSDBDao dao = iterator.next();
			if (dao.isSlave())
				continue;
			ret = dao.decr(key,decr);
			if (this.isAsyn)
				break;
		}
		if (iterator.hasNext()) {
			Runnable run = new Runnable() {
				@Override
				public void run() {
					while (iterator.hasNext()) {
						AbstractSSDBDao dao = iterator.next();
						if (dao.isSlave()) continue;
						dao.decr(key,decr);
					}
				}
			};
			this.invoke.exec(run);
		}
		return ret;
	}

	@Override
	public boolean expire(final String key, final int liveSeconds) {
		boolean ret = false;
		final Iterator<AbstractSSDBDao> iterator = caches.iterator();
		while (iterator.hasNext()) {
			AbstractSSDBDao dao = iterator.next();
			if (dao.isSlave())
				continue;
			ret = dao.expire(key,liveSeconds);
			if (this.isAsyn)
				break;
		}
		if (iterator.hasNext()) {
			Runnable run = new Runnable() {
				@Override
				public void run() {
					while (iterator.hasNext()) {
						AbstractSSDBDao dao = iterator.next();
						if (dao.isSlave()) continue;
						dao.expire(key,liveSeconds);
					}
				}
			};
			this.invoke.exec(run);
		}
		return ret;
	}

	@Override
	public long getExpireTime(String key) {
		long r = 0;
		if (CacheClusterConfig.ALG_POLLING.equals(this.clusterAlg)) {
			AbstractSSDBDao dao = this.alg.getServerObject();
			r = dao.getExpireTime(key);
    		if (log.isDebugEnabled()) {
    			log.debug("clusterAlg==" + this.clusterAlg + " SSDB client " + dao.getName());
    		}	
    		
    	} else if (CacheClusterConfig.ALG_HASH.equals(this.clusterAlg)) {
    		AbstractSSDBDao dao = this.alg.getServerObject(key);
    		r = dao.getExpireTime(key);
    		if (log.isDebugEnabled()) {
    			log.debug("clusterAlg==" + this.clusterAlg + " SSDB client " + dao.getName());
    		}	
    	} else {
    		List<AbstractSSDBDao> list = this.alg.getAllServerObject();
    		int si = this.alg.getServerIndex();
    		AbstractSSDBDao dao = list.get(si);
    		r = dao.getExpireTime(key);
    		if (r > 0) {
                return r;
            }
    		
    		for (int i=0;i<list.size();i++) {
    			if (i != si) {
    				dao = list.get(i);
    	    		r = dao.getExpireTime(key);
    	    		if (r > 0) {
    	                break;
    	            }
    			}
    		}
    	}
		
		return r;
	}

	@Override
	public boolean zset(final String setName, final String key, final long score) {
		boolean ret = false;
		final Iterator<AbstractSSDBDao> iterator = caches.iterator();
		while (iterator.hasNext()) {
			AbstractSSDBDao dao = iterator.next();
			if (dao.isSlave())
				continue;
			ret = dao.zset(setName,key,score);
			if (this.isAsyn)
				break;
		}
		if (iterator.hasNext()) {
			Runnable run = new Runnable() {
				@Override
				public void run() {
					while (iterator.hasNext()) {
						AbstractSSDBDao dao = iterator.next();
						if (dao.isSlave()) continue;
						dao.zset(setName,key,score);
					}
				}
			};
			this.invoke.exec(run);
		}
		return ret;
	}

	@Override
	public boolean zdel(String setName, String key) {
		boolean ret = false;
		final Iterator<AbstractSSDBDao> iterator = caches.iterator();
		while (iterator.hasNext()) {
			AbstractSSDBDao dao = iterator.next();
			if (dao.isSlave())
				continue;
			ret = dao.zdel(setName,key);
		}
		
		return ret;
	}

	@Override
	public Long zget(String setName, String key) {
		
		long r = 0;
		if (CacheClusterConfig.ALG_POLLING.equals(this.clusterAlg)) {
			AbstractSSDBDao dao = this.alg.getServerObject();
			r = dao.zget(setName,key);
			if (log.isDebugEnabled()) {
				log.debug("clusterAlg==" + this.clusterAlg + " SSDB client "+ dao.getName());
			}

		} else if (CacheClusterConfig.ALG_HASH.equals(this.clusterAlg)) {
			AbstractSSDBDao dao = this.alg.getServerObject(key);
			r = dao.zget(setName,key);
			if (log.isDebugEnabled()) {
				log.debug("clusterAlg==" + this.clusterAlg + " SSDB client "+ dao.getName());
			}
		} else {
			List<AbstractSSDBDao> list = this.alg.getAllServerObject();
    		int si = this.alg.getServerIndex();
    		AbstractSSDBDao dao = list.get(si);
    		r = dao.zget(setName,key);
    		if (r > 0) {
                return r;
            }
    		for (int i=0;i<list.size();i++) {
    			if (i != si) {
    				dao = list.get(i);
    	    		r = dao.zget(setName,key);
    	    		if (r > 0) {
    	                break;
    	            }
    			}
    		}
		}

		return r;
	}

	@Override
	public long zincr(final String setName, final String key, final long incr) {
		long ret = 0;
		final Iterator<AbstractSSDBDao> iterator = caches.iterator();
		while (iterator.hasNext()) {
			AbstractSSDBDao dao = iterator.next();
			if (dao.isSlave())
				continue;
			ret = dao.zincr(setName,key,incr);
			if (this.isAsyn)
				break;
		}
		if (iterator.hasNext()) {
			Runnable run = new Runnable() {
				@Override
				public void run() {
					while (iterator.hasNext()) {
						AbstractSSDBDao dao = iterator.next();
						if (dao.isSlave()) continue;
						dao.zincr(setName,key,incr);
					}
				}
			};
			this.invoke.exec(run);
		}
		return ret;
	}

	@Override
	public long zdecr(final String setName, final String key, final long decr) {
		long ret = 0;
		final Iterator<AbstractSSDBDao> iterator = caches.iterator();
		while (iterator.hasNext()) {
			AbstractSSDBDao dao = iterator.next();
			if (dao.isSlave())
				continue;
			ret = dao.zdecr(setName,key,decr);
			if (this.isAsyn)
				break;
		}
		if (iterator.hasNext()) {
			Runnable run = new Runnable() {
				@Override
				public void run() {
					while (iterator.hasNext()) {
						AbstractSSDBDao dao = iterator.next();
						if (dao.isSlave()) continue;
						dao.zdecr(setName,key,decr);
					}
				}
			};
			this.invoke.exec(run);
		}
		return ret;
	}

	@Override
	public long zcount(final String setName, final String start, final String end) {
		long ret = 0;
		final Iterator<AbstractSSDBDao> iterator = caches.iterator();
		while (iterator.hasNext()) {
			AbstractSSDBDao dao = iterator.next();
			if (dao.isSlave())
				continue;
			ret = dao.zcount(setName,start,end);
			if (this.isAsyn)
				break;
		}
		if (iterator.hasNext()) {
			Runnable run = new Runnable() {
				@Override
				public void run() {
					while (iterator.hasNext()) {
						AbstractSSDBDao dao = iterator.next();
						if (dao.isSlave()) continue;
						 dao.zcount(setName,start,end);
					}
				}
			};
			this.invoke.exec(run);
		}
		return ret;
	}

	@Override
	public boolean zclear(String setName) {
		boolean ret = false;
		final Iterator<AbstractSSDBDao> iterator = caches.iterator();
		while (iterator.hasNext()) {
			AbstractSSDBDao dao = iterator.next();
			if (dao.isSlave())
				continue;
			ret = dao.zclear(setName);			
		}
		return ret;
	}

	@Override
	public boolean hdel(String hashName, String key) {
		boolean ret = false;
		final Iterator<AbstractSSDBDao> iterator = caches.iterator();
		while (iterator.hasNext()) {
			AbstractSSDBDao dao = iterator.next();
			if (dao.isSlave())
				continue;
			ret = dao.hdel(hashName,key);			
		}
		return ret;
	}

	@Override
	public long hsize(String hashName) {

		long r = 0;
		if (CacheClusterConfig.ALG_POLLING.equals(this.clusterAlg)) {
			AbstractSSDBDao dao = this.alg.getServerObject();
			r = dao.hsize(hashName);
			if (log.isDebugEnabled()) {
				log.debug("clusterAlg==" + this.clusterAlg + " SSDB client "+ dao.getName());
			}

		} else if (CacheClusterConfig.ALG_HASH.equals(this.clusterAlg)) {
			AbstractSSDBDao dao = this.alg.getServerObject(hashName);
			r = dao.hsize(hashName);
			if (log.isDebugEnabled()) {
				log.debug("clusterAlg==" + this.clusterAlg + " SSDB client "+ dao.getName());
			}
		} else {
			List<AbstractSSDBDao> list = this.alg.getAllServerObject();
    		int si = this.alg.getServerIndex();
    		AbstractSSDBDao dao = list.get(si);
    		r = dao.hsize(hashName);
    		if (r > 0) {
                return r;
            }
    		for (int i=0;i<list.size();i++) {
    			if (i != si) {
    				dao = list.get(i);
    	    		r = dao.hsize(hashName);
    	    		if (r > 0) {
    	                break;
    	            }
    			}
    		}
		}

		return r;
	}

	@Override
	public boolean hclear(String hashName) {
		boolean ret = false;
		final Iterator<AbstractSSDBDao> iterator = caches.iterator();
		while (iterator.hasNext()) {
			AbstractSSDBDao dao = iterator.next();
			if (dao.isSlave())
				continue;
			ret = dao.hclear(hashName);			
		}
		return ret;
	}

	@Override
	public String getHost(String key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ICachedConfigManager getCacheManager() {
		return this.cacheManager;
	}

	@Override
	public boolean keyExists(String key) {
		return this.isExist(key);
	}

	@Override
	public boolean delete(String key) {
		boolean ret = false;
		final Iterator<AbstractSSDBDao> iterator = caches.iterator();
		while (iterator.hasNext()) {
			AbstractSSDBDao dao = iterator.next();
			if (dao.isSlave())
				continue;
			ret = dao.delete(key);			
		}
		return ret;
	}

	



	@Override
	public boolean add(final String key, final Object value, final Date expiry) {
		boolean ret = false;
		final Iterator<AbstractSSDBDao> iterator = caches.iterator();
		while (iterator.hasNext()) {
			AbstractSSDBDao dao = iterator.next();
			if (dao.isSlave())
				continue;
			ret = dao.add(key, value,expiry);
			if (this.isAsyn)
				break;
		}
		if (iterator.hasNext()) {
			Runnable run = new Runnable() {
				@Override
				public void run() {
					while (iterator.hasNext()) {
						AbstractSSDBDao dao = iterator.next();
						if (dao.isSlave()) continue;
						dao.add(key, value,expiry);
					}
				}
			};
			this.invoke.exec(run);
		}
		return ret;
	}

	@Override
	public Object[] getMultiArray(String[] keys) {
		
		List<Object> ret = new ArrayList<Object> ();
		for (String key:keys) {
			Object o = this.get(key);
			if (o != null) {
				ret.add(o);
			}
		}
		
		
	    return ret.toArray(new Object[0]);
	}

	@Override
	public Map<String, Object> getMulti(String[] keys) {
		Map<String,Object> map =  new HashMap<String,Object> ();
		for (String key:keys) {
			Object o = this.get(key);
			if (o != null) {
				map.put(key, o);
			}
		}
		
		return map;
	}

	@Override
	public int getWeight() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean pingCheck() {
		if (this.failover == false) return true;
		List<AbstractSSDBDao> tmp = new ArrayList<AbstractSSDBDao> ();
		
		for (AbstractSSDBDao ch: caches) {
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
				AbstractSSDBDao dao = (AbstractSSDBDao)ch;
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

	private boolean ping (AbstractSSDBDao ch) {
		boolean r = true;
		try {
			 r = ch.pingCheck();
		}catch (Exception e) {
			log.error("pingCheck",e);
			r=false;
		}
		
		return r;
	}
	 private boolean compareCaches(List<AbstractSSDBDao> tmp) {
	    	if (tmp.size() != caches.size()) return false;
	    	for(IMemCached ch:caches) {
	    		boolean f = false;
	    		for (AbstractSSDBDao t:tmp) {
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
	
	@Override
	public void setWeight(int weight) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() {
		for(AbstractSSDBDao c: caches) {
			c.close();
		}
		
	}

	@Override
	public void init() {
		if (CacheClusterConfig.ALG_HASH.equals(this.clusterAlg)) {
    		int weights [] = new int [caches.size()];
    		Map<String,AbstractSSDBDao> map = new HashMap<String,AbstractSSDBDao>();
    		for (int i=0;i<caches.size();i++) {
    			weights[i] = caches.get(i).getWeight();
    			map.put(caches.get(i).getName(), caches.get(i));
    		}
    		alg = new ConsistentHash<AbstractSSDBDao>(weights, map);
		} else {

			int weights[] = new int[caches.size()];
			for (int i = 0; i < caches.size(); i++) {
				weights[i] = caches.get(i).getWeight();
			}
			alg = new WeightedRoundRobinScheduling<AbstractSSDBDao>(weights, caches);

		}
		
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
		 ArrayList<AbstractSSDBDao> tmp = new ArrayList<AbstractSSDBDao> ();
		for (IMemCached ch : caches) {
			tmp.add((AbstractSSDBDao) ch);
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

	@Override
	public void setCacheManager(ICachedConfigManager cacheManager) {
		this.cacheManager=cacheManager;
	}

	@Override
	public String getClusterAlg() {
		return this.clusterAlg;
	}

	@Override
	public void setClusterAlg(String clusterAlg) {
		this.clusterAlg=clusterAlg;
		
	}

	@Override
	public boolean isFailover() {
		return this.failover;
	}

	@Override
	public void setFailover(boolean failover) {
		this.failover=failover;
	}

	@Override
	public void setName(String name) {
		this.name=name;
	}

	@Override
	public String getName() {
		return this.name;
	}

	@Override
	public boolean setBytes(final String key, final byte[] bytes, final int liveSeconds) {
		boolean ret = false;
		final Iterator<AbstractSSDBDao> iterator = caches.iterator();
		while (iterator.hasNext()) {
			AbstractSSDBDao dao = iterator.next();
			if (dao.isSlave())
				continue;
			ret = dao.setBytes(key, bytes, liveSeconds);
			if (this.isAsyn)
				break;
		}
		if (iterator.hasNext()) {
			Runnable run = new Runnable() {
				@Override
				public void run() {
					while (iterator.hasNext()) {
						AbstractSSDBDao dao = iterator.next();
						if (dao.isSlave()) continue;
						dao.setBytes(key, bytes, liveSeconds);
					}
				}
			};
			this.invoke.exec(run);
		}
		return ret;
	}

	@Override
	public byte[] getBytes(String key) {
		byte r [] = null;
		if (CacheClusterConfig.ALG_POLLING.equals(this.clusterAlg)) {
			AbstractSSDBDao dao = this.alg.getServerObject();
			r = dao.getBytes(key);
    		if (log.isDebugEnabled()) {
    			log.debug("clusterAlg==" + this.clusterAlg + " SSDB client " + dao.getName());
    		}	
    		
    	} else if (CacheClusterConfig.ALG_HASH.equals(this.clusterAlg)) {
    		AbstractSSDBDao dao = this.alg.getServerObject(key);
    		r = dao.getBytes(key);
    		if (log.isDebugEnabled()) {
    			log.debug("clusterAlg==" + this.clusterAlg + " SSDB client " + dao.getName());
    		}	
    	} else {
    		List<AbstractSSDBDao> list = this.alg.getAllServerObject();
    		int si = this.alg.getServerIndex();
    		AbstractSSDBDao dao = list.get(si);
    		r = dao.getBytes(key);
    		if (r != null && r.length > 0) {
                return r;
            }
    		
    		for (int i=0;i<list.size();i++) {
    			if (i != si) {
    				dao = list.get(i);
    				r = dao.getBytes(key);
    	    		if (r != null && r.length > 0) {
    	                return r;
    	            }
    			}
    		}
    	}
		
		return r;
	}

	@Override
	public boolean hsetBytes(final String hashName, final String key, final byte[] bytes) {
		boolean ret = false;
		final Iterator<AbstractSSDBDao> iterator = caches.iterator();
		while (iterator.hasNext()) {
			AbstractSSDBDao dao = iterator.next();
			if (dao.isSlave())
				continue;
			ret = dao.hsetBytes(hashName,key, bytes);
			if (this.isAsyn)
				break;
		}
		
		if (iterator.hasNext()) {
			Runnable run = new Runnable() {
				@Override
				public void run() {
					while (iterator.hasNext()) {
						AbstractSSDBDao dao = iterator.next();
						if (dao.isSlave()) continue;
						dao.hsetBytes(hashName,key, bytes);
					}
				}
			};
			this.invoke.exec(run);
		}
		return ret;
	}

	@Override
	public byte[] hgetBytes(String hashName, String key) {
		byte r [] = null;
		if (CacheClusterConfig.ALG_POLLING.equals(this.clusterAlg)) {
			AbstractSSDBDao dao = this.alg.getServerObject();
			r = dao.hgetBytes(hashName,key);
    		if (log.isDebugEnabled()) {
    			log.debug("clusterAlg==" + this.clusterAlg + " SSDB client " + dao.getName());
    		}	
    		
    	} else if (CacheClusterConfig.ALG_HASH.equals(this.clusterAlg)) {
    		AbstractSSDBDao dao = this.alg.getServerObject(hashName);
    		r = dao.hgetBytes(hashName,key);
    		if (log.isDebugEnabled()) {
    			log.debug("clusterAlg==" + this.clusterAlg + " SSDB client " + dao.getName());
    		}	
    	} else {
    		List<AbstractSSDBDao> list = this.alg.getAllServerObject();
    		int si = this.alg.getServerIndex();
    		AbstractSSDBDao dao = list.get(si);
    		r = dao.hgetBytes(hashName,key);
    		if (r != null && r.length > 0) {
                return r;
            }
    		
    		for (int i=0;i<list.size();i++) {
    			if (i != si) {
    				dao = list.get(i);
    				dao.hgetBytes(hashName,key);
    	    		if (r != null && r.length > 0) {
    	                return r;
    	            }
    			}
    		}
    	}
		
		return r;
	}

	@Override
	protected boolean setBytes(final String key, final byte[] bytes, final int seconds, final int flag) {
		boolean ret = false;
		final Iterator<AbstractSSDBDao> iterator = caches.iterator();
		while (iterator.hasNext()) {
			AbstractSSDBDao dao = iterator.next();
			if (dao.isSlave())
				continue;
			ret = dao.setBytes(key, bytes, seconds,flag);
			if (this.isAsyn)
				break;
		}
		if (iterator.hasNext()) {
			Runnable run = new Runnable() {
				@Override
				public void run() {
					while (iterator.hasNext()) {
						AbstractSSDBDao dao = iterator.next();
						if (dao.isSlave()) continue;
						dao.setBytes(key, bytes, seconds,flag);
					}
				}
			};
			this.invoke.exec(run);
		}
		return ret;
	}

	public boolean isAsyn() {
		return isAsyn;
	}

	public void setAsyn(boolean isAsyn) {
		this.isAsyn = isAsyn;
	}

	@Override
	public boolean flushAll(String[] servers) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Map<byte [], byte[]> hgetallkvByte (String hashName) {
		Map<byte [], byte[]> r = null;
		if (CacheClusterConfig.ALG_POLLING.equals(this.clusterAlg)) {
			AbstractSSDBDao dao = this.alg.getServerObject();
			r = dao.hgetallkvByte(hashName);
    		if (log.isDebugEnabled()) {
    			log.debug("clusterAlg==" + this.clusterAlg + " SSDB client " + dao.getName());
    		}	
    		
    	} else if (CacheClusterConfig.ALG_HASH.equals(this.clusterAlg)) {
    		AbstractSSDBDao dao = this.alg.getServerObject(hashName);
    		r = dao.hgetallkvByte(hashName);
    		if (log.isDebugEnabled()) {
    			log.debug("clusterAlg==" + this.clusterAlg + " SSDB client " + dao.getName());
    		}	
    	} else {
    		
    		List<AbstractSSDBDao> list = this.alg.getAllServerObject();
    		int si = this.alg.getServerIndex();
    		AbstractSSDBDao dao = list.get(si);
    		r = dao.hgetallkvByte(hashName);
    		if (r != null && r.size() > 0) {
                return r;
            }
    		
    		for (int i=0;i<list.size();i++) {
    			if (i != si) {
    				dao = list.get(i);
    				r = dao.hgetallkvByte (hashName);
    	    		if (r != null && r.size() > 0) {
    	                break;
    	            }
    			}
    		}
    	}
		
		return r;
	}

	@Override
	public List<String> keys(String keyStart, String keyEnd, int limit) {
		 List<String> r = null;
		if (CacheClusterConfig.ALG_POLLING.equals(this.clusterAlg)) {
			AbstractSSDBDao dao = this.alg.getServerObject();
			r = dao.keys(keyStart, keyEnd, limit);
    		if (log.isDebugEnabled()) {
    			log.debug("clusterAlg==" + this.clusterAlg + " SSDB client " + dao.getName());
    		}	
    		
    	} else if (CacheClusterConfig.ALG_HASH.equals(this.clusterAlg)) {
    		AbstractSSDBDao dao = this.alg.getServerObject(keyStart);
    		r = dao.keys(keyStart, keyEnd, limit);
    		if (log.isDebugEnabled()) {
    			log.debug("clusterAlg==" + this.clusterAlg + " SSDB client " + dao.getName());
    		}	
    	} else {
    		
    		List<AbstractSSDBDao> list = this.alg.getAllServerObject();
    		int si = this.alg.getServerIndex();
    		AbstractSSDBDao dao = list.get(si);
    		r = dao.keys(keyStart, keyEnd, limit);
    		if (r != null && r.size() > 0) {
                return r;
            }
    		
    		for (int i=0;i<list.size();i++) {
    			if (i != si) {
    				dao = list.get(i);
    				r = dao.keys(keyStart, keyEnd, limit);
    	    		if (r != null && r.size() > 0) {
    	                break;
    	            }
    			}
    		}
    	}
		
		return r;
	}

	@Override
	public boolean multiSetKVByte (final Map<byte[], byte[]> map) {
		boolean ret = false;
		final Iterator<AbstractSSDBDao> iterator = caches.iterator();
		while (iterator.hasNext()) {
			AbstractSSDBDao dao = iterator.next();
			if (dao.isSlave())
				continue;
			ret = dao.multiSetKVByte(map);
			if (this.isAsyn)
				break;
		}
		if (iterator.hasNext()) {
			Runnable run = new Runnable() {
				@Override
				public void run() {
					while (iterator.hasNext()) {
						AbstractSSDBDao dao = iterator.next();
						if (dao.isSlave()) continue;
						 dao.multiSetKVByte(map);
					}
				}
			};
			this.invoke.exec(run);
		}
		return ret;
	}

	@Override
	public boolean multiHsetKVbytes (final String hashName, final Map<byte[], byte[]> map) {
		boolean ret = false;
		final Iterator<AbstractSSDBDao> iterator = caches.iterator();
		while (iterator.hasNext()) {
			AbstractSSDBDao dao = iterator.next();
			if (dao.isSlave())
				continue;
			ret = dao.multiHsetKVbytes(hashName,map);
			if (this.isAsyn)
				break;
		}
		if (iterator.hasNext()) {
			Runnable run = new Runnable() {
				@Override
				public void run() {
					while (iterator.hasNext()) {
						AbstractSSDBDao dao = iterator.next();
						if (dao.isSlave()) continue;
						 dao.multiHsetKVbytes (hashName,map);
					}
				}
			};
			this.invoke.exec(run);
		}
		return ret;
	}

	

	@Override
	public Map<byte[], byte[]> multiHgetKVBytes(String hashName, String[] keys) {
		Map<byte[], byte[] > r = null;
		if (CacheClusterConfig.ALG_POLLING.equals(this.clusterAlg)) {
			AbstractSSDBDao dao = this.alg.getServerObject();
			r = dao.multiHgetKVBytes (hashName,keys);
    		if (log.isDebugEnabled()) {
    			log.debug("clusterAlg==" + this.clusterAlg + " SSDB client " + dao.getName());
    		}	
    		
    	} else if (CacheClusterConfig.ALG_HASH.equals(this.clusterAlg)) {
    		AbstractSSDBDao dao = this.alg.getServerObject(hashName);
    		r = dao.multiHgetKVBytes(hashName,keys);
    		if (log.isDebugEnabled()) {
    			log.debug("clusterAlg==" + this.clusterAlg + " SSDB client " + dao.getName());
    		}	
    	} else {
    		
    		List<AbstractSSDBDao> list = this.alg.getAllServerObject();
    		int si = this.alg.getServerIndex();
    		AbstractSSDBDao dao = list.get(si);
    		r = dao.multiHgetKVBytes (hashName,keys);
    		if (r != null && r.size() > 0) {
                return r;
            }
    		
    		for (int i=0;i<list.size();i++) {
    			if (i != si) {
    				dao = list.get(i);
    				r = dao.multiHgetKVBytes(hashName,keys);
    	    		if (r != null && r.size() > 0) {
    	                break;
    	            }
    			}
    		}
    	}
		
		return r;
	}

	@Override
	public boolean multiHdel(String hashName, String[] key) {
		boolean ret = false;
		final Iterator<AbstractSSDBDao> iterator = caches.iterator();
		while (iterator.hasNext()) {
			AbstractSSDBDao dao = iterator.next();
			if (dao.isSlave())
				continue;
			ret = dao.multiHdel(hashName,key);
			/*if (this.isAsyn)
				break;*/
		}
		
		return ret;
	}

	@Override
	public Map<byte[], byte []> hScanKVBytes (String hashName, String keyStart,String keyEnd, int limit) {
		 Map<byte [], byte []> r = null;
		 
		 if (CacheClusterConfig.ALG_POLLING.equals(this.clusterAlg)) {
				AbstractSSDBDao dao = this.alg.getServerObject();
				r = dao.hScanKVBytes(hashName, keyStart, keyEnd, limit);
	    		if (log.isDebugEnabled()) {
	    			log.debug("clusterAlg==" + this.clusterAlg + " SSDB client " + dao.getName());
	    		}	
	    		
	    	} else if (CacheClusterConfig.ALG_HASH.equals(this.clusterAlg)) {
	    		AbstractSSDBDao dao = this.alg.getServerObject(hashName);
	    		r = dao.hScanKVBytes(hashName, keyStart, keyEnd, limit);
	    		if (log.isDebugEnabled()) {
	    			log.debug("clusterAlg==" + this.clusterAlg + " SSDB client " + dao.getName());
	    		}	
	    	} else {
	    		
	    		List<AbstractSSDBDao> list = this.alg.getAllServerObject();
	    		int si = this.alg.getServerIndex();
	    		AbstractSSDBDao dao = list.get(si);
	    		r = dao.hScanKVBytes(hashName, keyStart, keyEnd, limit);
	    		if (r != null && r.size() > 0) {
	                return r;
	            }
	    		
	    		for (int i=0;i<list.size();i++) {
	    			if (i != si) {
	    				dao = list.get(i);
	    				r = dao.hScanKVBytes(hashName, keyStart, keyEnd, limit);
	    	    		if (r != null && r.size() > 0) {
	    	                break;
	    	            }
	    			}
	    		}
	    	}
		 
		return r;
	}

	@Override
	public List<byte[]> multiGetByte(String[] keys) {
		List<byte []> r = null;
		if (CacheClusterConfig.ALG_POLLING.equals(this.clusterAlg)) {
			AbstractSSDBDao dao = this.alg.getServerObject();
			r = dao.multiGetByte(keys);
    		if (log.isDebugEnabled()) {
    			log.debug("clusterAlg==" + this.clusterAlg + " SSDB client " + dao.getName());
    		}	
    		
    	} else if (CacheClusterConfig.ALG_HASH.equals(this.clusterAlg)) {
    		AbstractSSDBDao dao = this.alg.getServerObject(keys[0]);
    		r = dao.multiGetByte(keys);
    		if (log.isDebugEnabled()) {
    			log.debug("clusterAlg==" + this.clusterAlg + " SSDB client " + dao.getName());
    		}	
    	} else {
    		
    		List<AbstractSSDBDao> list = this.alg.getAllServerObject();
    		int si = this.alg.getServerIndex();
    		AbstractSSDBDao dao = list.get(si);
    		r = dao.multiGetByte(keys);
    		if (r != null && r.size() > 0) {
                return r;
            }
    		
    		for (int i=0;i<list.size();i++) {
    			if (i != si) {
    				dao = list.get(i);
    				r = dao.multiGetByte(keys);
    	    		if (r != null && r.size() > 0) {
    	                break;
    	            }
    			}
    		}
    	}
		
		return r;
	}


	@Override
	public boolean multiDel(final String[] keys) {
		boolean ret = false;
		final Iterator<AbstractSSDBDao> iterator = caches.iterator();
		while (iterator.hasNext()) {
			AbstractSSDBDao dao = iterator.next();
			if (dao.isSlave())
				continue;
			ret = dao.multiDel(keys);
			/*if (this.isAsyn)
				break;*/
		}
		/*if (iterator.hasNext()) {
			Runnable run = new Runnable() {
				@Override
				public void run() {
					while (iterator.hasNext()) {
						AbstractSSDBDao dao = iterator.next();
						if (dao.isSlave()) continue;
						dao.multiDel(keys);
					}
				}
			};
			this.invoke.exec(run);
		}*/
		return ret;
	}

	@Override
	public List<String> hkeys(String hashName, String keyStart, String keyEnd,int limit) {
		List<String> r = null;
		if (CacheClusterConfig.ALG_POLLING.equals(this.clusterAlg)) {
			AbstractSSDBDao dao = this.alg.getServerObject();
			r = dao.hkeys(hashName, keyStart, keyEnd, limit);
    		if (log.isDebugEnabled()) {
    			log.debug("clusterAlg==" + this.clusterAlg + " SSDB client " + dao.getName());
    		}	
    		
    	} else if (CacheClusterConfig.ALG_HASH.equals(this.clusterAlg)) {
    		AbstractSSDBDao dao = this.alg.getServerObject(hashName);
    		r = dao.hkeys(hashName, keyStart, keyEnd, limit);
    		if (log.isDebugEnabled()) {
    			log.debug("clusterAlg==" + this.clusterAlg + " SSDB client " + dao.getName());
    		}	
    	} else {
    		
    		List<AbstractSSDBDao> list = this.alg.getAllServerObject();
    		int si = this.alg.getServerIndex();
    		AbstractSSDBDao dao = list.get(si);
    		r = dao.hkeys(hashName, keyStart, keyEnd, limit);
    		if (r != null && r.size() > 0) {
                return r;
            }
    		
    		for (int i=0;i<list.size();i++) {
    			if (i != si) {
    				dao = list.get(i);
    				r = dao.hkeys(hashName, keyStart, keyEnd, limit);
    	    		if (r != null && r.size() > 0) {
    	                break;
    	            }
    			}
    		}
    	}
		
		return r;
	}
}
