package com.dhgate.ssdb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dhgate.Hashing;
import com.dhgate.ICachedConfigManager;
import com.dhgate.memcache.core.PoolConfig;
import com.dhgate.memcache.schooner.SchoonerSockIOPool;
import com.dhgate.redis.AbstractRedisDao;
import com.dhgate.ssdb.client.SSDB;

public class SSDBCache extends AbstractSSDBDao {
	private static Logger log = LoggerFactory.getLogger(SSDBCache.class);
	
	private ICachedConfigManager cacheManager;
	private String name;
	//private volatile int keyLenDefault = AbstractRedisDao.KEY_LEN_DEFAULT; //byte 
	//private volatile int valueSizeDefault = AbstractRedisDao.VALUE_SIZE_DEFAULT; //byte
	private int hashingAlg = SchoonerSockIOPool.CONSISTENT_HASH;
	private String servers[];
	private int weights [];
	private PoolConfig config;
	private int weight =1;
	
	
	private  Map<String, SSDBPool> socketPools ;
	private final Hashing algo =Hashing.MURMUR_HASH;
	
	private List<String> buckets;
	private TreeMap<Long, String> consistentBuckets;
	
	
	public SSDBCache (String name,String[] servers, int[] weights, PoolConfig config,int hashingAlg) {
		super();
		this.name = name;
		this.servers = servers;
		this.weights = weights;
		this.config = config;
		this.hashingAlg=hashingAlg;
		this.init();
	}
	
	
	@Override
	public ICachedConfigManager getCacheManager() {
		return this.cacheManager;
	}

	@Override
	public boolean keyExists(String key) {
		if (key == null) {
			return false;
		}

		SSDB ssdb = null;
		boolean ret = false;
		boolean broken = false;
		try {
			ssdb = getSSDB(key);
			if (ssdb != null) {
				ret = ssdb.isExist(key);
			}
		} catch (Exception e) {
			broken = handleException(e);
			String shardInfo="";
			if (ssdb != null && ssdb.getPool() != null)
				shardInfo = ssdb.getPool().getHost()+":"+ssdb.getPool().getPort();
			log.error(shardInfo + " key:"+key+" Error occurred when isExist", e);
		} finally {
			closeSSDB(ssdb, broken);
		}
		return ret;
	}

	@Override
	public boolean delete(String key) {
		boolean ret = false;
		if (key == null) {
			return ret;
		}
		SSDB ssdb = null;

		boolean broken = false;
		try {
			ssdb = getSSDB(key);
			if (ssdb != null) {
				ret = ssdb.del(key);
			}
		} catch (Exception e) {
			broken = handleException(e);
			String shardInfo="";
			if (ssdb != null && ssdb.getPool() != null)
				shardInfo = ssdb.getPool().getHost()+":"+ssdb.getPool().getPort();
			log.error(shardInfo + " key:"+key+" Error occurred when del", e);
		} finally {
			closeSSDB(ssdb, broken);
		}
		return ret;
	}

	
	
	
	@Override
	protected boolean setBytes(String key, byte[] bytes, int seconds,int flag) {
		boolean ret = false;
		if (key == null || bytes == null)
			return ret;
		
		SSDB client = null;
		/** flag indicating whether the connection needs to be dropped or not */
		 boolean broken = false;
		try {
			client = getSSDB(key);
			if(client != null){
				switch (flag) {
				    case 0:
					    if (seconds <= 0) {
						    ret = client.set(key, bytes);
					    } else {
						    ret = client.setex(key,bytes,seconds);
					    }
					  break;
				    case 1:
				    	
				    	ret = client.setnx (key, bytes, seconds) ;
				    	
				    	break;
				    case 2:
				    	byte b [] = client.get(key);
				    	if (b != null && b.length > 0) {
					    	if (seconds <= 0) {
					    		ret = client.set(key, bytes);
					    	} else {
					    		ret = client.setex(key, bytes,  seconds) ;//add
					    	}
				    	}
				    	
				    	break;
				    default:
				    	ret = false;
				}
				
			}
		} catch (Exception e) {
 			broken = handleException(e);
 			
 			String shardInfo="";
 			if (client != null && client.getPool() != null)
				shardInfo = client.getPool().getHost()+":"+client.getPool().getPort();
 			log.error(shardInfo +" key:"+key+ " Error occurred when setBytes", e);
		} finally {
			this.closeSSDB(client, broken);
		}
		
		return ret;
	}
	

	@Override
	public byte[] getBytes(String key) {
		if (key == null) {
			return null;
		}

		SSDB ssdb = null;
		
		
		byte[] ret = null;
		boolean broken = false;
		try {
			ssdb = getSSDB(key);
			if (ssdb != null) {
				ret = ssdb.get(key);
			}
		} catch (Exception e) {
			broken = handleException(e);
			String shardInfo="";
			if (ssdb != null && ssdb.getPool() != null)
				shardInfo = ssdb.getPool().getHost()+":"+ssdb.getPool().getPort();
			
			log.error(shardInfo + " key:"+key+" Error occurred when getBytes", e);
		} finally {
			closeSSDB(ssdb, broken);
		}
		return ret;
	}
	
	
	@Override
	public Object[] getMultiArray(String[] keys) {
		List<Object> list =  new ArrayList<Object> ();
		for (String key:keys) {
			Object o = this.get(key);
			if (o != null) {
				list.add(o);
			}
		}
		
		return list.toArray();
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
	public String getName() {
		return this.name;
	}

	@Override
	public int getWeight() {
		return this.weight;
	}

	@Override
	public boolean pingCheck() {
		if (socketPools == null) return false;
		boolean f = false;
		boolean broken = false;
		for ( SSDBPool sp: socketPools.values()) {
			SSDB io = sp.getResource();
			try {
			 f =  io.ping();
			} catch (Exception e) {
				broken = this.handleException(e);
				log.error("ping failed for " + sp.toString(),e);
			} finally {
				closeSSDB(io,broken);
			}
			
			if (!f) return f;
		}
		
		return f;
	}

	@Override
	public void setName(String name) {
		this.name=name;
		
	}

	@Override
	public void setWeight(int weight) {
		this.weight=weight;
		
	}

	@Override
	public synchronized void close() {
		for (SSDBPool i : socketPools.values()) {
			try {
				i.destroy();
			} catch (Exception e) {
				
				log.error("closeSocketPool", e);
			}
		}
		
		this.socketPools.clear();
		this.socketPools=null;
		if (this.buckets != null)
			this.buckets.clear();
		if (this.consistentBuckets != null)
			this.consistentBuckets.clear();
		
	}

	public String[] getServers() {
		return servers;
	}

	public void setServers(String[] servers) {
		this.servers = servers;
	}

	public int[] getWeights() {
		return weights;
	}

	public void setWeights(int[] weights) {
		this.weights = weights;
	}

	public void setCacheManager(ICachedConfigManager cacheManager) {
		this.cacheManager = cacheManager;
	}
	
	private  synchronized void init (){
		try {

			// if servers is not set, or it empty, then
			// throw a runtime exception
			if (servers == null || servers.length <= 0) {
				log.error("++++ trying to initialize with no servers");
				throw new IllegalStateException("++++ trying to initialize with no servers");
			}
			
			socketPools = new HashMap<String, SSDBPool>(servers.length);  			
			if (this.hashingAlg == SchoonerSockIOPool.CONSISTENT_HASH)
				populateConsistentBuckets();
			else
				populateBuckets();

		} catch (Exception e) {
			log.error("initialize failed ",e);
		}
	}
	
	private void populateConsistentBuckets() throws Exception {
		// store buckets in tree map
		consistentBuckets = new TreeMap<Long, String>();

		for (int i = 0; i < servers.length; i++) {
			int thisWeight = 1;
			if (this.weights != null && this.weights[i] > 0)
				thisWeight = this.weights[i];
			String host = servers[i].trim();
			SSDBPool pool = this.createSSDBPool(host);
			for (int n = 0; n < 160 * thisWeight; n++) {
				consistentBuckets.put(this.algo.hash("SHARD-" + i + "-NODE-" + n), host);
			}

			socketPools.put(host, pool);
		}
	}
	
	private void populateBuckets() throws Exception {
		
		buckets = new ArrayList<String>();
		for (int i = 0; i < servers.length; i++) {
			String host = servers[i].trim();
			if (this.weights != null && this.weights.length > i) {
				for (int k = 0; k < this.weights[i]; k++) {
					buckets.add(host);
				}
			} else {
				buckets.add(host);
			}

			SSDBPool pool = this.createSSDBPool(host); 
			socketPools.put(host, pool);
		}
	}
	
	public synchronized void updatePool (String servers[],int hashingAlg,int wegits[],PoolConfig config) {
	
		try {

			if (servers == null || servers.length <= 0) {
				log.error("++++ trying to initialize with no servers");
				throw new IllegalStateException("++++ trying to initialize with no servers");
			}			
			
			boolean needUp=false;
			if ( servers.length != this.servers.length) {
				needUp = true;
			} else {
				for (int i=0;i<servers.length;i++) {
					if (! servers[i].trim().equals(this.servers[i].trim())) {
						needUp=true;
						break;
					}
				}
			}
			
		    if ( needUp || (config.getTimeout() > 0 && config.getTimeout() != this.config.getTimeout())) {
		    	distroyPoll(this.servers);
		    	this.servers=servers;
		    	this.hashingAlg=hashingAlg;
		    	this.weights=wegits;
		    	this.config=config;
		    	
				if (hashingAlg == SchoonerSockIOPool.CONSISTENT_HASH)
					populateConsistentBuckets();
				else
					populateBuckets();
				
				log.info("++++finished updatePool for cache name "  + this.getName());
		    } else if (config.isDifferent(this.config)) {
		    	this.config=config;
		    	for (SSDBPool sp:socketPools.values()) {
		    		sp.setConfig(config);
		    	}
		    } 
		} catch (Exception e) {
			log.error("initialize failed ",e);
		}

	}
	
	private void distroyPoll(String[] keys) {

		for (String key : keys) {
			SSDBPool pool = socketPools.remove(key);
			try {

				if (pool != null)
					pool.destroy();
				pool = null;

			} catch (Exception e) {
				log.error("distroyPoll :" + key, e);
			}
		}

	}
	
	private SSDBPool createSSDBPool(String hostInfo) {
		SSDBPool pool = null;
		int pos = hostInfo.indexOf(":");
		if (pos > 0) {
			String ip = hostInfo.substring(0, pos);
			int port = Integer.valueOf(hostInfo.substring(pos + 1).trim());
			pool = new SSDBPool(config, ip, port, config.getTimeout());
		} else {
			pool = new SSDBPool(config, hostInfo, 8888, config.getTimeout());
		}
		return pool;
	}
	
	private void closeSSDB(SSDB ssdb, boolean broken) {
		if (ssdb != null) {
			if (!broken) {
				ssdb.getPool().returnResource(ssdb);
			} else {
				ssdb.getPool().returnBrokenResource(ssdb);
			}
		} else {
			log.error("The ssdb instance is null when close with poolkey " );
		}
	}
	
	private boolean handleException(Exception e) {
		//log.error("handleException",e);
		boolean broken = false;
		if (e instanceof SSDBException || e instanceof IOException) {
			broken = true;
		}
		return broken;
	}
	
	private SSDB getSSDB(String key) {
		SSDB ssdb = null;
		if (socketPools.values().size() == 1) {
			return socketPools.values().iterator().next().getResource();
		}
		String host = null;
		Long kl = algo.hash(key);
		if (this.hashingAlg == SchoonerSockIOPool.CONSISTENT_HASH) {
			SortedMap<Long, String> tail = this.consistentBuckets.tailMap(kl);
			if (tail.isEmpty()) {
				 host = consistentBuckets.get(consistentBuckets.firstKey());
			} else {
			 host = tail.get(tail.firstKey());
			}
			
		} else {
			int bucket = kl.intValue() % buckets.size();
			if (bucket < 0)
				bucket *= -1;
			host = buckets.get(bucket);
		}
		SSDBPool pool = socketPools.get(host);
		if (pool != null) {
			ssdb = pool.getResource();
		} else {
			log.error("The ssdb pool is null with key " + key + " cache name:" + this.getName());
		}

		return ssdb;
	}


	@Override
	public boolean del(String key) {
		boolean ret = false;
		if (key == null) {
			return ret;
		}
		SSDB ssdb = null;

		boolean broken = false;
		try {
			ssdb = getSSDB(key);
			if (ssdb != null) {
				ret = ssdb.del(key);
			}
		} catch (Exception e) {
			broken = handleException(e);
			String shardInfo="";
			if (ssdb != null && ssdb.getPool() != null)
				shardInfo = ssdb.getPool().getHost()+":"+ssdb.getPool().getPort();
			log.error(shardInfo + " key:"+key+" Error occurred when del", e);
		} finally {
			closeSSDB(ssdb, broken);
		}
		return ret;
	}


	@Override
	public boolean isExist(String key) {
		if (key == null) {
			return false;
		}

		SSDB ssdb = null;
		boolean ret = false;
		boolean broken = false;
		try {
			ssdb = getSSDB(key);
			if (ssdb != null) {
				ret = ssdb.isExist(key);
			}
		} catch (Exception e) {
			broken = handleException(e);
			String shardInfo="";
			if (ssdb != null && ssdb.getPool() != null)
				shardInfo = ssdb.getPool().getHost()+":"+ssdb.getPool().getPort();
			log.error(shardInfo + " key:"+key+" Error occurred when isExist", e);
		} finally {
			closeSSDB(ssdb, broken);
		}
		return ret;
	}


	@Override
	public long incr(String key, long incr) {
		if (key == null) {
			return 0;
		}

		SSDB ssdb = null;
		long ret = 0l;
		boolean broken = false;
		try {
			ssdb = getSSDB(key);
			if (ssdb != null) {
				ret = ssdb.incr(key, incr);
			}
		} catch (Exception e) {
			broken = handleException(e);
			String shardInfo="";
			if (ssdb != null && ssdb.getPool() != null)
				shardInfo = ssdb.getPool().getHost()+":"+ssdb.getPool().getPort();
			log.error(shardInfo + " key:"+key+" Error occurred when incr", e);
		} finally {
			closeSSDB(ssdb, broken);
		}
		return ret;
	}


	@Override
	public long decr(String key, long decr) {
		if (key == null) {
			return 0;
		}

		SSDB ssdb = null;
		long ret = 0l;
		boolean broken = false;
		try {
			ssdb = getSSDB(key);
			if (ssdb != null) {
				ret = ssdb.decr(key, decr);
			}
		} catch (Exception e) {
			broken = handleException(e);
			String shardInfo="";
			if (ssdb != null && ssdb.getPool() != null)
				shardInfo = ssdb.getPool().getHost()+":"+ssdb.getPool().getPort();
			log.error(shardInfo + " key:"+key+" Error occurred when decr", e);
			
		} finally {
			closeSSDB(ssdb, broken);
		}
		return ret;
	}


	@Override
	public boolean expire(String key, int liveSeconds) {
		boolean ret = false;
		if (key == null) {
			return ret;
		}

		SSDB ssdb = null;
		boolean broken = false;
		try {
			ssdb = getSSDB(key);
			if (ssdb != null) {
				ret = ssdb.expire(key, liveSeconds);
			}
		} catch (Exception e) {
			broken = handleException(e);
			String shardInfo="";
			if (ssdb != null && ssdb.getPool() != null)
				shardInfo = ssdb.getPool().getHost()+":"+ssdb.getPool().getPort();
			log.error(shardInfo + " key:"+key+" Error occurred when expire", e);
			
		} finally {
			closeSSDB(ssdb, broken);
		}
		return ret;
	}


	@Override
	public long getExpireTime(String key) {
		long ret = 0l;
		if (key == null) {
			return ret;
		}

		SSDB ssdb = null;
		boolean broken = false;
		try {
			ssdb = getSSDB(key);
			if (ssdb != null) {
				ret = ssdb.ttl(key);
			}
		} catch (Exception e) {
			broken = handleException(e);
			String shardInfo="";
			if (ssdb != null && ssdb.getPool() != null)
				shardInfo = ssdb.getPool().getHost()+":"+ssdb.getPool().getPort();
			log.error(shardInfo + " key:"+key+" Error occurred when getExpireTime", e);
			
		} finally {
			closeSSDB(ssdb, broken);
		}
		return ret;
	}


	@Override
	public boolean zset(String setName, String key, long score) {
		boolean ret = false;
		if (key == null) {
			return ret;
		}

		SSDB ssdb = null;
		boolean broken = false;
		try {
			ssdb = getSSDB(key);
			if (ssdb != null) {
				ret = ssdb.zset(setName, key, score);
			}
		} catch (Exception e) {
			broken = handleException(e);
			log.error("Error occurred when zset", e);
		} finally {
			closeSSDB(ssdb, broken);
		}
		return ret;
	}


	@Override
	public boolean zdel(String setName, String key) {
		boolean ret = false;
		if (key == null) {
			return ret;
		}

		SSDB ssdb = null;
		boolean broken = false;
		try {
			ssdb = getSSDB(key);
			if (ssdb != null) {
				ret = ssdb.zdel(setName, key);
			}
		} catch (Exception e) {
			broken = handleException(e);
			log.error("Error occurred when zdel", e);
		} finally {
			closeSSDB(ssdb, broken);
		}
		return ret;
	}


	@Override
	public Long zget(String setName, String key) {
		Long ret = null;
		if (key == null) {
			return ret;
		}

		SSDB ssdb = null;
		boolean broken = false;
		try {
			ssdb = getSSDB(key);
			if (ssdb != null) {
				ret = ssdb.zget(setName, key);
			}
		} catch (Exception e) {
			broken = handleException(e);
			log.error("Error occurred when zget", e);
		} finally {
			closeSSDB(ssdb, broken);
		}
		return ret;
	}


	@Override
	public long zincr(String setName, String key, long incr) {
		long ret = 0l;
		if (key == null) {
			return ret;
		}

		SSDB ssdb = null;
		boolean broken = false;
		try {
			ssdb = getSSDB(key);
			if (ssdb != null) {
				ret = ssdb.zincr(setName, key, incr);
			}
		} catch (Exception e) {
			broken = handleException(e);
			log.error("Error occurred when zincr", e);
		} finally {
			closeSSDB(ssdb, broken);
		}
		return ret;
	}


	@Override
	public long zdecr(String setName, String key, long decr) {
		long ret = 0l;
		if (key == null) {
			return ret;
		}

		SSDB ssdb = null;
		boolean broken = false;
		try {
			ssdb = getSSDB(key);
			if (ssdb != null) {
				ret = ssdb.zdecr(setName, key, decr);
			}
		} catch (Exception e) {
			broken = handleException(e);
			log.error("Error occurred when zdecr", e);
		} finally {
			closeSSDB(ssdb, broken);
		}
		return ret;
	}


	@Override
	public long zcount(String setName, String start, String end) {
		long ret = 0l;
		if (setName == null) {
			return ret;
		}
		if (start == null) {
			start = EMPTY_STRING;
		}
		if (end == null) {
			end = EMPTY_STRING;
		}

		boolean broken = false;

		for (SSDBPool pool : socketPools.values()) {
			SSDB ssdb = pool.getResource();
			try {
				if (ssdb != null) {
					ret+= ssdb.zcount(setName, start, end);
				}
			} catch (Exception e) {
				broken = handleException(e);
				log.error("Error occurred when zcount", e);
			} finally {
				closeSSDB(ssdb, broken);
			}
		}

		return ret;
	}


	@Override
	public boolean zclear(String setName) {
		boolean ret = false;
		if (setName == null) {
			return ret;
		}

		boolean broken = false;

		for (SSDBPool pool : socketPools.values()) {
			SSDB ssdb = pool.getResource();
			try {
				if (ssdb != null) {
					ret = ssdb.zclear(setName);
				}
			} catch (Exception e) {
				broken = handleException(e);
				log.error("Error occurred when zclear", e);
			} finally {
				closeSSDB(ssdb, broken);
			}
		}

		return ret;
	}


	@Override
	public boolean hdel(String hashName, String key) {
		boolean ret = false;
		if (key == null) {
			return ret;
		}

		SSDB ssdb = null;
		boolean broken = false;
		try {
			ssdb = getSSDB(hashName);
			if (ssdb != null) {
				ret = ssdb.hdel(hashName, key);
			}
		} catch (Exception e) {
			broken = handleException(e);
			log.error("Error occurred when hdel", e);
		} finally {
			closeSSDB(ssdb, broken);
		}
		return ret;
	}

	@Override
	public long hsize(String hashName) {
		long ret = 0l;
		if (hashName == null) {
			return ret;
		}
		
		boolean broken = false;
		SSDB ssdb = getSSDB(hashName);
		try {
			if (ssdb != null) {
				ret = ssdb.hsize(hashName);
			}
		} catch (Exception e) {
			broken = handleException(e);
			log.error("Error occurred when hsize", e);
		} finally {
			closeSSDB(ssdb, broken);
		}

		return ret;
	}

	@Override
	public boolean hclear(String hashName) {
		boolean ret = false;
		if (hashName == null) {
			return ret;
		}

		boolean broken = false;

		SSDB ssdb = getSSDB(hashName);
		try {
			if (ssdb != null) {
				ret = ssdb.hclear(hashName);
			}
		} catch (Exception e) {
			broken = handleException(e);
			log.error("Error occurred when hclear", e);
		} finally {
			closeSSDB(ssdb, broken);
		}

		return ret;
	}


	@Override
	public String getHost(String key) {
		SSDB ssdb =  this.getSSDB(key);
		if (ssdb == null) {
			log.error("The ssdb pool is null with key ==" + key);
		}
		
		String host = ssdb.getPool().getHost()+":"+ssdb.getPool().getPort();
		closeSSDB(ssdb,false);
		return host;
	}


	@Override
	public boolean setBytes(String key, byte[] bytes, int liveSeconds) {
		return this.setBytes(key, bytes, liveSeconds, 0);
	}


	@Override
	public boolean hsetBytes(String hashName, String key, byte[] bytes) {
		boolean ret = false;
		SSDB ssdb = null;
		boolean broken = false;
		try {
			ssdb = getSSDB(hashName);
			if (ssdb != null) {
				ret = ssdb.hset(hashName, key, bytes);
			}
		} catch (Exception e) {
			broken = handleException(e);
			log.error("Error occurred when hsetBytes", e);
		} finally {
			closeSSDB(ssdb, broken);
		}
		return ret;
	}


	@Override
	public byte[] hgetBytes(String hashName, String key) {
		byte[] ret = null;
		if (key == null) {
			return ret;
		}

		SSDB ssdb = null;		
		boolean broken = false;
		try {
			ssdb = getSSDB(hashName);
			if (ssdb != null) {
				ret = ssdb.hget(hashName, key);
			}
		} catch (Exception e) {
			broken = handleException(e);
			log.error("Error occurred when hgetBytes", e);
		} finally {
			closeSSDB(ssdb, broken);
		}
		return ret;
	}


	@Override
	public boolean flushAll(String[] servers) {
		// TODO Auto-generated method stub
		return false;
	}


	@Override
	public List<String> keys(String keyStart, String keyEnd, int limit) {
		List<String> r = new ArrayList<String>(limit);
		boolean broken = false;
		for (SSDBPool pool : socketPools.values()) {
			SSDB ssdb = pool.getResource();
			try {
				if (ssdb != null) {
					 ssdb.keys(keyStart, keyEnd, limit,r);
					if (r.size() >= limit) {
						return r;
					} else {
						limit -= r.size();
					}
				}
			} catch (Exception e) {
				broken = handleException(e);
				log.error("Error occurred when hsize", e);
			} finally {
				closeSSDB(ssdb, broken);
			}
		}
		
		return r;
	}

	@Override
	public boolean multiHdel(String hashName, String[] keys) {
		boolean ret = false;
		if (hashName == null) {
			return ret;
		}
		SSDB ssdb = null;		
		boolean broken = false;
		try {
			ssdb = getSSDB(hashName);
			if (ssdb != null) {
				ret  = ssdb.multiHdel(hashName,keys);
			}
		} catch (Exception e) {
			broken = handleException(e);
			log.error("Error occurred when multiHdel", e);
		} finally {
			closeSSDB(ssdb, broken);
		}
		return ret;
	}

	@Override
	public boolean multiDel(String[] keys) {
		boolean r = false ;
		boolean broken = false;
		if (socketPools.values().size() == 1) {
			SSDB ssdb = socketPools.values().iterator().next().getResource();
			try {
				return ssdb.multiDel(keys);
			} catch (Exception e) {
				broken = handleException(e);
				log.error("Error occurred when multiDel ", e);
			} finally {
				closeSSDB(ssdb, broken);
			}
		}
		
		Map<SSDBPool, List<String>> temp = new HashMap<SSDBPool, List<String>>(socketPools.size());
		SSDB ssdb = null;

		for (String key : keys) {
			ssdb = getSSDB(key);
			List<String> list = temp.get(ssdb.getPool());
			if (list == null) {
				list = new ArrayList<String>();
				temp.put(ssdb.getPool(), list);
			}
			list.add(key);
		}
		
		for (SSDBPool pool : temp.keySet()) {
			try {
				ssdb = pool.getResource();
				r = ssdb.multiDel(temp.get(pool).toArray(new String[0]));
			} catch (Exception e) {
				broken = handleException(e);
				log.error("Error occurred when multiDel ", e);
			} finally {
				closeSSDB(ssdb, broken);

			}
		}
			
		return r;
	}


	@Override
	public List<String> hkeys(String hashName, String keyStart, String keyEnd,int limit) {
		List<String> r = null;
		boolean broken = false;
		
			SSDB ssdb = null;
			try {
				ssdb = this.getSSDB(hashName);
				if (ssdb != null) {
					 r = ssdb.HKeys(hashName,keyStart, keyEnd, limit);
				}
			} catch (Exception e) {
				broken = handleException(e);
				log.error("Error occurred when hkeys", e);
			} finally {
				closeSSDB(ssdb, broken);
			}
		
		
		return r;
	}


	
    @Override
	protected Map<byte[], byte[]> hgetallkvByte (String hashName) {
		Map<byte [],byte[]> ret = new HashMap<byte [],byte[]> ();
		if (hashName == null) {
			return ret;
		}

		SSDB ssdb = null;		
		boolean broken = false;
		try {
			ssdb = getSSDB(hashName);
			if (ssdb != null) {
				ret  = ssdb.hgetAll (hashName);
			}
		} catch (Exception e) {
			broken = handleException(e);
			log.error("Error occurred when hgetall", e);
		} finally {
			closeSSDB(ssdb, broken);
		}
		return ret;
	}
	
	

	@Override
	protected Map<byte[], byte[]> multiHgetKVBytes(String hashName, String[] keys) {
		Map<byte[],byte[]> ret = new HashMap<byte[],byte[]> ();
		if (hashName == null) {
			return ret;
		}
		SSDB ssdb = null;		
		boolean broken = false;
		try {
			ssdb = getSSDB(hashName);
			if (ssdb != null) {
				ret  = ssdb.multiHget(hashName,keys);
			}
		} catch (Exception e) {
			broken = handleException(e);
			log.error("Error occurred when multiHset", e);
		} finally {
			closeSSDB(ssdb, broken);
		}
		return ret;
	}
	

	@Override
	public List<byte[]> multiGetByte(String[] keys) {

		List<byte []> r = new ArrayList<byte []>();
		boolean broken = false;
		if (socketPools.values().size() == 1) {
			SSDB ssdb = socketPools.values().iterator().next().getResource();
			try {
				Map<byte[], byte[]> map = ssdb.multiGet(keys);
				if (map != null) {
					for (byte[] buf : map.values()) {
						r.add(buf);
					}
				}
				return r;
			} catch (Exception e) {
				broken = handleException(e);
				log.error("Error occurred when multiGet ", e);
			} finally {
				closeSSDB(ssdb, broken);
			}
		}

		Map<SSDBPool, List<String>> temp = new HashMap<SSDBPool, List<String>>(
				socketPools.size());
		SSDB ssdb = null;

		for (String key : keys) {
			ssdb = getSSDB(key);
			List<String> list = temp.get(ssdb.getPool());
			if (list == null) {
				list = new ArrayList<String>();
				temp.put(ssdb.getPool(), list);
			}
			list.add(key);
		}

		for (SSDBPool pool : temp.keySet()) {
			try {
				ssdb = pool.getResource();
				Map<byte[], byte[]> map = ssdb.multiGet(temp.get(pool).toArray(
						new String[0]));
				if (map != null) {
					for (byte[] buf : map.values()) {
						r.add(buf);
					}
				}
			} catch (Exception e) {
				broken = handleException(e);
				log.error("Error occurred when multiGet ", e);
			} finally {
				closeSSDB(ssdb, broken);

			}
		}

		return r;
	}

	@Override
	protected Map<byte[], byte[]> hScanKVBytes(String hashName, String keyStart,String keyEnd, int limit) {
		Map<byte[],byte[]> ret = new HashMap<byte[],byte[]> ();
		if (hashName == null) {
			return ret;
		}
		SSDB ssdb = null;		
		boolean broken = false;
		try {
			ssdb = getSSDB(hashName);
			if (ssdb != null) {
				ret  = ssdb.hscan(hashName, keyStart, keyEnd, limit);
			}
		} catch (Exception e) {
			broken = handleException(e);
			log.error("Error occurred when multiHset", e);
		} finally {
			closeSSDB(ssdb, broken);
		}
		return ret;
	}


	
   
    @Override
	protected boolean multiSetKVByte(Map<byte[], byte[]> map) {
    	boolean r = false ;
		boolean broken = false;
		if (socketPools.values().size() == 1) {
			SSDB ssdb = socketPools.values().iterator().next().getResource();
			try {
			
			List<byte[]> kvs = new ArrayList<byte[]> (map.size()*2);
			
			for (byte[] k : map.keySet()) {
				
				kvs.add(k);
				kvs.add(map.get(k));
			}
			return ssdb.multiSet(kvs.toArray(kvs.toArray(new byte[0][0])));
			} catch (Exception e) {
				broken = handleException(e);
				log.error("Error occurred when multiSet ", e);
			} finally {
				closeSSDB(ssdb, broken);
			}
		}
		
		Map<SSDBPool, List<byte[]>> temp = new HashMap<SSDBPool, List<byte[]>>(socketPools.size());
		SSDB ssdb = null;

		for (byte [] key : map.keySet()) {
			ssdb = getSSDB(new String(key));
			List<byte[]> list = temp.get(ssdb.getPool());
			if (list == null) {
				list = new ArrayList<byte[]>();
				temp.put(ssdb.getPool(), list);
			}
				list.add(key);
				list.add(map.get(key));	
		}
		
		for (SSDBPool pool : temp.keySet()) {
			try {
				ssdb = pool.getResource();
				r = ssdb.multiSet(temp.get(pool).toArray(new byte[0][0]));
			} catch (Exception e) {
				broken = handleException(e);
				log.error("Error occurred when multiSet ", e);
			} finally {
				closeSSDB(ssdb, broken);

			}
		}
			
		return r;
    }
	
	
	@Override
	protected boolean multiHsetKVbytes (String hashName, Map<byte[], byte[]> map) {
		boolean r = false;
		boolean broken = false;

		SSDB ssdb = null;
		try {
			ssdb = getSSDB(hashName);
			List<byte[]> kvs = new ArrayList<byte[]>((map.size() * 2) + 1);
			kvs.add(hashName.getBytes(AbstractRedisDao.DEFAULT_ENCODING));
		

			for (byte [] k : map.keySet()) {			
				kvs.add(k);
				kvs.add(map.get(k));
			}
			return ssdb.multiHset(kvs.toArray(kvs.toArray(new byte[0][0])));
		} catch (Exception e) {
			broken = handleException(e);
			log.error("Error occurred when multiHset ", e);
		} finally {
			closeSSDB(ssdb, broken);
		}

		return r;
	}
	
	
}
