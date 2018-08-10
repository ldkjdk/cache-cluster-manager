package com.dhgate.redis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dhgate.ICachedConfigManager;
import com.dhgate.SafeEncoder;
import com.dhgate.memcache.schooner.SchoonerSockIOPool;
import com.dhgate.redis.clients.jedis.Jedis;
import com.dhgate.redis.clients.jedis.JedisPoolConfig;
import com.dhgate.redis.clients.jedis.JedisPubSub;
import com.dhgate.redis.clients.jedis.JedisShardInfo;
import com.dhgate.redis.clients.jedis.Protocol;
import com.dhgate.redis.clients.jedis.ShardedJedis;
import com.dhgate.redis.clients.jedis.ShardedJedisPipeline;
import com.dhgate.redis.clients.jedis.ShardedJedisPool;
import com.dhgate.redis.clients.jedis.exceptions.JedisConnectionException;
import com.dhgate.redis.clients.util.Sharded;

/**
 * 
 * @author lidingkun
 *
 */

public class RedisCache extends  AbstractRedisDao {
	
	private static final Logger log = LoggerFactory.getLogger(RedisCache.class);
	
	
	private ICachedConfigManager cacheManager;
	private ShardedJedisPool redisPool = null;
	private String name;
	private volatile int keyLenDefault = AbstractRedisDao.KEY_LEN_DEFAULT; //byte 
	private volatile int valueSizeDefault = AbstractRedisDao.VALUE_SIZE_DEFAULT; //byte
	private String servers[];
	private int weights [];
	private JedisPoolConfig config;
	private int timeout;
	private int weight =1;
	
	public RedisCache(String name,String[] servers, int[] weights, JedisPoolConfig config, int timeout) {
		super();
		this.name = name;
		this.servers = servers;
		this.weights = weights;
		this.config = config;
		this.timeout = timeout;
		this.init();
	}

	@Override
	public ICachedConfigManager getCacheManager() {
		return cacheManager;
	}

	@Override
	public boolean keyExists(String key) {
		if (key == null){
			return false;
   	 	}
		
		boolean ret = isExistFromPool(key,0);
		return ret;
	}

	@Override
	public boolean delete(String key) {
		long ret = 0L;		
		ShardedJedis shardedJedis = null;			
		boolean broken = false;
		try {
			shardedJedis = redisPool.getResource();
			if(shardedJedis != null){
				Jedis client=shardedJedis.getShard(key);
				client.select(0);
				ret = client.del(key);
			}
		} catch (Exception e) {
 			broken = handleException(e);
			log.error(getHost(shardedJedis,key) + " Error occurred when delFromPool", e);
			ret= -1;
		} finally {
			closeShardedJedis(shardedJedis, broken);
		}
		
		return ret > 0;
	}

	private String getHost(ShardedJedis shardedJedis,String key) {
		if (shardedJedis == null || shardedJedis.getShardInfo(key) == null)
			return "";
		return shardedJedis.getShardInfo(key).getHost().toString();
	}
	private String getHost(ShardedJedis shardedJedis,byte[] key) {
		if (shardedJedis == null || shardedJedis.getShardInfo(key) == null)
			return "";
		return shardedJedis.getShardInfo(key).getHost().toString();
	}
	

	@Override
	public Object get(String key) {
		if (key == null)
			return null;
		byte[] ret = getBytes(key.getBytes(),0);
		if (ret == null)
			return null;
		return ObjectBytesExchange.toObject(ret);
	}

	@Override
	public Object[] getMultiArray(String[] keys) {
		List<Object> ret = getBytesByBatch(keys);
		if (ret != null) {
			return ret.toArray(new Object[0]);
		}
		return null;
	}

	@Override
	public Map<String, Object> getMulti(String[] keys) {
		
		if (keys == null || keys.length > 1000){
			log.error("The list is null or size is greater than 1000 in getStringByBatch, will not process.");
			return null;
		}
		
		List<Object> ret = getBytesByBatch(keys);
		Map<String,Object> rMap = new HashMap<String, Object>();
		if(ret != null && ret.size()==keys.length){
			
			for(int i=0;i<keys.length;i++){
				rMap.put(keys[i], ret.get(i));
			}
		}else{
			log.error("The return list is null in getStringByBatch.");
		}
		return rMap;
	}

	@Override
	public String getName() {
		return this.name;
	}

	@Override
	public void setName(String name) {
		this.name=name;
	}

	@Override
	public void close() {
		if (redisPool  != null)
			redisPool.destroy();
		
		redisPool = null;
		
	}
	
	private boolean isExistFromPool(String key, int index){
		ShardedJedis shardedJedis = null;
		boolean ret = false;
		/** flag indicating whether the connection needs to be dropped or not */
		 boolean broken = false;
		try {
			shardedJedis = redisPool.getResource();
			if(shardedJedis != null){
				Jedis client = shardedJedis.getShard(key);
				client.select(index);
				ret = client.exists(key);
			}
		} catch (Exception e) {
			broken = handleException(e);
			log.error(getHost(shardedJedis,key)+" Error occurred when isExist", e);
		} finally {
			closeShardedJedis(shardedJedis, broken);
		}
		return ret;
	}
	
	private List<Object> getBytesByBatch(String [] keyList) {
		
		ShardedJedis shardedJedis = null;
		List<Object> returnList = null;
		boolean broken = false;		
		try {
			shardedJedis = redisPool.getResource();
			if(shardedJedis != null){		
				ShardedJedisPipeline pipeline = shardedJedis.pipelined();
				for(String key : keyList){
					pipeline.get(key.getBytes(AbstractRedisDao.DEFAULT_ENCODING));
				}
				returnList = pipeline.syncAndReturnAll();		
			}
		} catch (Exception e) {
	 		broken = handleException(e);
			log.error(this.getName() + " Error occurred when getBytesByBatch", e);
		} finally {
			closeShardedJedis(shardedJedis, broken);
		}
		return returnList;
	}
	
	@Override
	public byte[] getBytes(byte[] key, int index) {
		if (key == null)
			return null;
		ShardedJedis shardedJedis = null;
		byte[] ret = null;
		/** flag indicating whether the connection needs to be dropped or not */
		 boolean broken = false;
			try {
				
				shardedJedis = redisPool.getResource();	
				Jedis client=shardedJedis.getShard(key);
				client.select(index);
				ret = client.get(key);
				return ret;
			} catch (Exception e) {
	 			broken = handleException(e);
	 			log.error(getHost(shardedJedis,key)+"  Error occurred when getBytes from slave", e);
			} finally {
				closeShardedJedis(shardedJedis,broken);
			}
		
		return ret;
	}
	
	protected boolean setBytes(byte[] key, byte[] bytes, int seconds, int index,int flag) {
		if (key == null || bytes == null)
			return false;
		if (key.length > this.keyLenDefault || bytes.length > this.valueSizeDefault) {
			log.error ("key or value size more than default.");
			return false;
		}
		
		ShardedJedis shardedJedis = null;
		String ret = null;
		
		/** flag indicating whether the connection needs to be dropped or not */
		 boolean broken = false;
		try {
			shardedJedis = redisPool.getResource();
			if(shardedJedis != null){
				Jedis client=shardedJedis.getShard(key);
				client.select(index);
				switch (flag) {
				    case 0:
					    if (seconds <= 0) {
						    ret = client.set(key, bytes);
					    } else {
						    ret = client.setex(key, seconds, bytes);
					    }
					    
					  break;
				    case 1:
				    	if (seconds <= 0) {
				    		ret = client.set(key, bytes, SafeEncoder.encode("NX")) ;
				    	} else {
				    		ret = client.set(key, bytes, SafeEncoder.encode("NX"), SafeEncoder.encode("EX"), seconds) ;//add
				    	}
				    	break;
				    case 2:
				    	
				    	if (seconds <= 0) {
				    		ret = client.set(key, bytes, SafeEncoder.encode("XX"));
				    	} else {
				    		ret = client.set(key, bytes, SafeEncoder.encode("XX"), SafeEncoder.encode("EX"), seconds) ;//add
				    	}
				    	
				    	break;
				    default:
				    	ret = null;
				}
				
			}
		} catch (Exception e) {
 			broken = handleException(e);
 			log.error(getHost(shardedJedis,key) +" Error occurred when setBytes", e);
		} finally {
			closeShardedJedis(shardedJedis, broken);
		}
		
		return (AbstractRedisDao.SUCCESS_SET.equals(ret));
	}
	
	private void closeShardedJedis(ShardedJedis shardedJedis, boolean broken) {
    	if(shardedJedis != null){
	    	if(!broken){	
				redisPool.returnResource(shardedJedis);	
			} else {
				redisPool.returnBrokenResource(shardedJedis);
			}
    	}
    }
	
	private boolean handleException(Exception e){
    	boolean broken = false; 
    	if(e instanceof JedisConnectionException || e instanceof IOException){
 			broken=true;
 		}
    	return broken;
    }

	public ShardedJedisPool getRedisPool() {
		return redisPool;
	}

	public void setRedisPool(ShardedJedisPool redisPool) {
		this.redisPool = redisPool;
	}


	public void setCacheManager(ICachedConfigManager cacheManager) {
		this.cacheManager = cacheManager;
	}
	
	
	
	public String[] getServers() {
		return servers;
	}

	public void setServers(String[] servers) {
		this.servers = servers;
	}

	

	public JedisPoolConfig getConfig() {
		return config;
	}

	public void setConfig(JedisPoolConfig config) {
		this.config = config;
	}

	
	public int getTimeout() {
		return timeout;
	}

	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}

	
	public int [] getWeights() {
		return weights;
	}

	public void setWeights(int [] weights) {
		this.weights = weights;
	}
	
	
    
	private  synchronized void init (){
		
		List<JedisShardInfo> shardJedis = new ArrayList<JedisShardInfo>();
		for (int i = 0; i < servers.length; i++) {		
			JedisShardInfo jedisShardInfo;
			String host = servers[i];
			int weight =  Sharded.DEFAULT_WEIGHT;
			if (weights != null && weights.length >i) {
				weight = weights[i];
			}
			int pos = host.indexOf(AbstractRedisDao.HOST_PORT_SEPERATOR);
			if (host != null && pos > 0){
				String ip = host.substring(0,pos);
				int port = Integer.parseInt(host.substring(pos+1).trim()); //trim some space
				jedisShardInfo=new JedisShardInfo(ip, port,timeout,weight);
			}else{
				jedisShardInfo=new JedisShardInfo(host,Protocol.DEFAULT_PORT,timeout,weight);
			}
			shardJedis.add(jedisShardInfo);
		}
		
		if (this.getHashingAlg() == SchoonerSockIOPool.CONSISTENT_HASH)
			this.redisPool = new ShardedJedisPool(this.config, shardJedis,false);
		else
			this.redisPool = new ShardedJedisPool(this.config, shardJedis,true);
	}
	@Override
	public synchronized void updatePool (String servers[],int hashingAlg,int weights[],JedisPoolConfig conf,int timeout) {
		
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
			
			
		    if ( needUp || (timeout  > 0 && timeout!= this.timeout)) {
		    	ShardedJedisPool tempPool= this.redisPool;
		    	this.servers=servers;
		    	//this.hashingAlg=hashingAlg;
		    	this.weights=weights;
		    	this.config=conf;
		    	
				this.init();
				tempPool.close();
				
				log.info("++++finished updatePool for name " + this.name);
		    } else if (isDifferent(conf)) {
		    	this.config=conf;
		    	this.redisPool.setConfig(this.config);
		    } 
		} catch (Exception e) {
			log.error("initialize failed ",e);
		}

	}
	
	public boolean isDifferent(JedisPoolConfig o) {
		if (o == null) return true;
		//(this.initialSize != o.getInitialSize()) ||
		 if ( (this.config.minIdle != o.getMinIdle()) 
				 || (this.config.timeBetweenEvictionRunsMillis != o.getTimeBetweenEvictionRunsMillis())
				 || (this.config.minEvictableIdleTimeMillis != o.getMinEvictableIdleTimeMillis())
				 || (this.config.numTestsPerEvictionRun != o.getNumTestsPerEvictionRun())
				 || ( this.config.maxIdle != o.getMaxActive()) 
				 || (this.config.maxActive != o.getMaxActive()) 
				 || ( this.config.maxWait != o.getMaxWait()) 
				 || (this.config.testOnBorrow != o.isTestOnBorrow())) 
			      return true;
        
       return false;
	}

	@Override
	public int getWeight() {
		
		return this.weight;
	}

	@Override
	public boolean pingCheck() {
		
		if (this.redisPool == null)
			return false;
		boolean broken = false;
		ShardedJedis sj = redisPool.getResource();
		try {
		for (Jedis jd:sj.getAllShards()) {
			
			if(AbstractRedisDao.PING_RESPONSE.equals(jd.ping())){
				return true;
			}
		}
		}catch (Exception e) {
 			broken = handleException(e);
 			log.error(this.getName() + " Error occurred when getBytes from slave", e);
		} finally {
			closeShardedJedis(sj,broken);
		}
		
		return false;
	}

	@Override
	public void setWeight(int weight) {
		this.weight=weight;
		
	}

	@Override
	public String setString(String key, String str, int liveSeconds, int index) {
		if (key == null || str == null)
			return null;
		if (key.getBytes().length > this.keyLenDefault || str.getBytes().length > this.valueSizeDefault) {
			log.error ("key or value size more than default.");
			return null;
		}
		ShardedJedis shardedJedis = null;
		String ret = null;
		/** flag indicating whether the connection needs to be dropped or not */
		 boolean broken = false;
		try {
			shardedJedis = this.redisPool.getResource();
			if(shardedJedis != null){
				Jedis client = shardedJedis.getShard(key);
				client.select(index);
				if (liveSeconds <= 0) {
					ret = client.set(key, str);
				}else{
					ret = client.setex(key, liveSeconds, str);
				}
			}
		} catch (Exception e) {
 			broken = handleException(e);
			log.error(getHost(shardedJedis,key) + " Error occurred when setString", e);		
		} finally {
			closeShardedJedis(shardedJedis, broken);
		}
		return ret;	}

	@Override
	public String getString(String key, int index) {
		ShardedJedis shardedJedis = null;
		String ret = null;
		/** flag indicating whether the connection needs to be dropped or not */
		boolean broken = false;
		
			try {
				shardedJedis = redisPool.getResource();	
				Jedis client =shardedJedis.getShard(key);
				client.select(index);
				ret = client.get(key);
				
				return ret;
			} catch (Exception e) {
	 			broken = handleException(e);
				log.error(getHost(shardedJedis,key) +" Error occurred when getString",e);
			} finally {
				closeShardedJedis(shardedJedis, broken);
			}
		return ret;
	}

	@Override
	public boolean delKeyByBatch(List<String> keyList, int index) {
		ShardedJedis shardedJedis = null;
		boolean ret = false;
		boolean broken = false;		
		
		try {
			shardedJedis = redisPool.getResource();
			if(shardedJedis != null){
				Collection<Jedis> t = shardedJedis.getAllShards();
				for(Jedis j : t){
					j.select(index);
				}				
				ShardedJedisPipeline pipeline = shardedJedis.pipelined();
				for(String key : keyList){
					pipeline.del(key);
				}
				List<Object> returnList = pipeline.syncAndReturnAll();		
				boolean foundError = false;
				for(Object object : returnList){
					if(object != null && ((Long)object).intValue() != 1){
						foundError = true;
						log.warn("There is not success operation in delKeyByBatch");
						break;
					}
				}
				if(!foundError){
					ret = true;
				}
			}
		} catch (Exception e) {
	 		broken = handleException(e);
			log.error(this.getName() +" Error occurred when delKeyByBatch", e);
		} finally {
			closeShardedJedis(shardedJedis, broken);
		}
		return ret;
	}

	@Override
	public boolean isExist(String key, int index) {
		
		if (key == null){
			return false;
   	 	}
		
		boolean ret = isExistFromPool(key, index);
		return ret;
	}

	@Override
	public Object eval(String script, List<String> keys, List<String> args) throws Exception {
		if (keys == null)
			return null;
		
		 String key = keys.get(0);
         Object result = null;
		 boolean broken = false;
		
		
		ShardedJedis shardedJedis = null;
		try {
			shardedJedis = this.redisPool.getResource();
			if(shardedJedis != null){
				Jedis client=shardedJedis.getShard(key);
				result=client.eval(script,keys,args);
			}
		} catch (Exception e) {
			broken = handleException(e);
			log.error(getHost(shardedJedis,key) + " Error occurred when lock", e);
			throw e;
		} finally {
			closeShardedJedis(shardedJedis, broken);
		}
		
		return result;
	}	

	@Override
	public long incr(String key, int index) {
		if (key == null){
			return 0L;
		}
		ShardedJedis shardedJedis = null;
		long ret = 0L;
		/** flag indicating whether the connection needs to be dropped or not */
		boolean broken = false;
		try {
			shardedJedis =this.redisPool.getResource();			
			if(shardedJedis != null){
				Jedis client=shardedJedis.getShard(key);
				client.select(index);
				ret = client.incr(key);
			}
		} catch (Exception e) {
 			broken = handleException(e);
			log.error(getHost(shardedJedis,key) + " Error occurred when incr", e);
			ret= -1;		
		} finally {
			closeShardedJedis(shardedJedis, broken);
		}
		return ret;
	}
	
	@Override
	public long incrBy(String key, long integer,int index) {
		if (key == null){
			return 0L;
		}
		ShardedJedis shardedJedis = null;
		long ret = 0L;
		/** flag indicating whether the connection needs to be dropped or not */
		boolean broken = false;
		try {
			shardedJedis =this.redisPool.getResource();			
			if(shardedJedis != null){
				Jedis client=shardedJedis.getShard(key);
				client.select(index);
				ret = client.incrBy(key, integer);
			}
		} catch (Exception e) {
 			broken = handleException(e);
			log.error(getHost(shardedJedis,key) + " Error occurred when incr", e);
			ret= -1;		
		} finally {
			closeShardedJedis(shardedJedis, broken);
		}
		return ret;
	}
	
	@Override
	public long decr(String key, int index) {
		if (key == null){
			return 0L;
		}
		ShardedJedis shardedJedis = null;
		long ret = 0L;
		/** flag indicating whether the connection needs to be dropped or not */
		boolean broken = false;
		try {
			shardedJedis =this.redisPool.getResource();			
			if(shardedJedis != null){
				Jedis client=shardedJedis.getShard(key);
				client.select(index);
				ret = client.decr(key);
			}
		} catch (Exception e) {
 			broken = handleException(e);
			log.error(getHost(shardedJedis,key) + " Error occurred when incr", e);
			ret= -1;		
		} finally {
			closeShardedJedis(shardedJedis, broken);
		}
		return ret;
	}
	
	@Override
	public long decrBy(String key,long integer, int index) {
		if (key == null){
			return 0L;
		}
		ShardedJedis shardedJedis = null;
		long ret = 0L;
		/** flag indicating whether the connection needs to be dropped or not */
		boolean broken = false;
		try {
			shardedJedis =this.redisPool.getResource();			
			if(shardedJedis != null){
				Jedis client=shardedJedis.getShard(key);
				client.select(index);
				ret = client.decrBy(key, integer);
			}
		} catch (Exception e) {
 			broken = handleException(e);
			log.error(getHost(shardedJedis,key) + " Error occurred when incr", e);
			ret= -1;		
		} finally {
			closeShardedJedis(shardedJedis, broken);
		}
		return ret;
	}
	

	@Override
	public long expire(String key, int seconds, int index) {
		if (key == null) {
			return 0L;
		}
		ShardedJedis shardedJedis = null;
		long ret = 0L;
		boolean broken = false;
		try {
			shardedJedis = this.redisPool.getResource();
			if(shardedJedis != null){
				Jedis client = shardedJedis.getShard(key);
				client.select(index);
				ret = client.expire(key, seconds);
			}
		} catch (Exception e) {
 			broken = handleException(e);
			log.error(getHost(shardedJedis,key) + " Error occurred when expire", e);
			ret= -1;
		} finally {		
			closeShardedJedis(shardedJedis, broken);
		}
		return ret;	
	}
	
	

	@Override
	public long expireAt(String key, long unixTime, int index) {
		if(key == null) {
			return 0;
		}
		
		long ret = 0L;
		ShardedJedis shardedJedis = null;
		boolean broken = false;
		try {
			shardedJedis = this.redisPool.getResource();
			if(shardedJedis != null){
				Jedis client = shardedJedis.getShard(key);
				client.select(index);
				ret = client.expireAt(key, unixTime);	
			}
		} catch (Exception e) {
 			broken = handleException(e);
			log.error(getHost(shardedJedis,key) + " Error occurred when expireAt", e);
			ret= -1;
		} finally {			
			closeShardedJedis(shardedJedis, broken);
		}
		return ret;
	}

	@Override
	public long getHashLength(String key, int index) {
		long ret = -1;
		ShardedJedis shardedJedis = null;
		boolean broken = false;
		if(key == null){
			return ret;
		}
		try {
			shardedJedis = this.redisPool.getResource();
			if(shardedJedis != null){
				Jedis client = shardedJedis.getShard(key);
				client.select(index);
				ret = client.hlen(key);
			}
		} catch (Exception e) {
 			broken = handleException(e);
			log.error(getHost(shardedJedis,key) + " Error occurred when getHashLength", e);
		} finally {			
			closeShardedJedis(shardedJedis, broken);
		}	
		return ret;
	}

	@Override
	public boolean isHashFieldExist(String key, String field, int index) {
		boolean ret = false;
		ShardedJedis shardedJedis = null;
		boolean broken = false;
		if(key == null){
			return ret;
		}
		try {
			shardedJedis = this.redisPool.getResource();
			if(shardedJedis != null){
				Jedis client = shardedJedis.getShard(key);
				client.select(index);
				ret = client.hexists(key, field);
			}
		} catch (Exception e) {
 			broken = handleException(e);
			log.error(getHost(shardedJedis,key) + " Error occurred when isHashFieldExist", e);
		} finally {			
			closeShardedJedis(shardedJedis, broken);
		}		
		return ret;
	}

	@Override
	public long getSetSize(String key, int index) {
		long ret = -1;
		ShardedJedis shardedJedis = null;
		boolean broken = false;
		if(key == null){
			return ret;
		}
		try {
			shardedJedis = this.redisPool.getResource();
			if(shardedJedis != null){
				Jedis client = shardedJedis.getShard(key);
				client.select(index);
				ret = client.scard(key);
			}
		} catch (Exception e) {
 			broken = handleException(e);
			log.error(getHost(shardedJedis,key) + " Error occurred when getSetSize", e);
		} finally {			
			closeShardedJedis(shardedJedis, broken);
		}	
		return ret;
	}

	@Override
	public Long publish(String key, String message) {
		if (key == null || message == null)
            return null;
        if (key.getBytes().length > this.keyLenDefault || message.getBytes().length > this.valueSizeDefault) {
            log.error ("key or value size more than default.");
            return null;
        }
        ShardedJedis shardedJedis = null;
        Long ret = null;
        /** flag indicating whether the connection needs to be dropped or not */
         boolean broken = false;
        try {
            shardedJedis = this.redisPool.getResource();
            if(shardedJedis != null){
                Jedis client = shardedJedis.getShard(key);
               // client.select(0);
                 ret = client.publish(key, message);
                
            }
        } catch (Exception e) {
            broken = handleException(e);
            log.error(getHost(shardedJedis,key) +  " Error occurred when setString", e);      
        } finally {
            closeShardedJedis(shardedJedis, broken);
        }
        return ret;
	}

	@Override
	public void subscribe(JedisPubSub jedisPubSub, String key) {
		if (key == null)
            return ;
        if (key.getBytes().length > this.keyLenDefault ) {
            log.error ("key or value size more than default.");
            return ;
        }
        ShardedJedis shardedJedis = null;
        
        /** flag indicating whether the connection needs to be dropped or not */
         boolean broken = false;
        try {
            shardedJedis = this.redisPool.getResource();
            if(shardedJedis != null){
                Jedis client = shardedJedis.getShard(key);
               // client.select(0);
               
                client.subscribe(jedisPubSub, key);
                
            }
        } catch (Exception e) {
            broken = handleException(e);
            log.error(getHost(shardedJedis,key) + " Error occurred when setString", e);      
        } finally {
            closeShardedJedis(shardedJedis, broken);
        }
		
	}

	@Override
	public Map<String, String> hgetAll(String key, int index) {
		 ShardedJedis shardedJedis = null;
	        Map<String,String> ret = null;
	        /** flag indicating whether the connection needs to be dropped or not */
	        boolean broken = false;
		try {
            shardedJedis = this.redisPool.getResource();
            if(shardedJedis != null){
                Jedis client =shardedJedis.getShard(key);
                //log.info ("TEST :"+key +" ="+client.toString()); for test
                client.select(index);
                ret = client.hgetAll(key);
            }
        } catch (Exception e) {
            broken = handleException(e);
            log.error(getHost(shardedJedis,key) +" Error occurred when getStringFromPool",e);
        } finally {
            closeShardedJedis(shardedJedis, broken);
        }       
       
		return ret;
	}

	@Override
	public Map<String, Object> getHashAllObject(String key, int index) {
		 ShardedJedis shardedJedis = null;
	        Map<byte[],byte []> ret = null;
	        /** flag indicating whether the connection needs to be dropped or not */
	        boolean broken = false;
	        try {
	            shardedJedis = this.redisPool.getResource();
	            if(shardedJedis != null){
	                Jedis client =shardedJedis.getShard(key);
	                //log.info ("TEST :"+key +" ="+client.toString()); for test
	                client.select(index);
	                ret = client.hgetAll(key.getBytes(AbstractRedisDao.DEFAULT_ENCODING));
	            }
	        } catch (Exception e) {
	            broken = handleException(e);
	            log.error(getHost(shardedJedis,key) + " Error occurred when getStringFromPool",e);
	        } finally {
	            closeShardedJedis(shardedJedis, broken);
	        } 
	        
	        return this.convert(ret);
	}

	@Override
	protected String setBytes(byte[] key, byte[] bytes, int seconds, int index) {
		if (key == null || bytes == null)
			return null;
		if (key.length > this.keyLenDefault || bytes.length > this.valueSizeDefault) {
			log.error ("key or value size more than default.");
			return null;
		}
		
		ShardedJedis shardedJedis = null;
		String ret = null;
		/** flag indicating whether the connection needs to be dropped or not */
		 boolean broken = false;
		try {
			shardedJedis = this.redisPool.getResource();
			if(shardedJedis != null){
				Jedis client=shardedJedis.getShard(key);
				client.select(index);
				if (seconds <= 0) {
					ret = client.set(key, bytes);
				} else {
					ret = client.setex(key, seconds, bytes);
				}
			}
		} catch (Exception e) {
 			broken = handleException(e);
 			log.error(getHost(shardedJedis,key) + " Error occurred when setBytes", e);
		} finally {
			closeShardedJedis(shardedJedis, broken);
		}
		return ret;
	}

	@Override
	protected long del(String key, int index) {
		if (key == null){
			return 0L;
		}
		long ret = 0L;		
		ShardedJedis shardedJedis = null;			
		boolean broken = false;
		try {
			shardedJedis = this.redisPool.getResource();
			if(shardedJedis != null){
				Jedis client=shardedJedis.getShard(key);
				client.select(index);
				ret = client.del(key);
			}
		} catch (Exception e) {
 			broken = handleException(e);
			log.error(getHost(shardedJedis,key) + " Error occurred when delFromPool", e);
			ret= -1;
		} finally {
			closeShardedJedis(shardedJedis, broken);
		}
		return ret;
	}

	@Override
	protected boolean setBytesByBatch(List<String> keyList,List<byte[]> valueList, int liveSeconds, int index) {
		ShardedJedis shardedJedis = null;
		boolean ret = false;
		boolean broken = false;		
		
		try {
			shardedJedis = this.redisPool.getResource();
			if(shardedJedis != null){
				Collection<Jedis> t = shardedJedis.getAllShards();
				for(Jedis j : t){
					j.select(index);
				}				
				ShardedJedisPipeline pipeline = shardedJedis.pipelined();
				if(liveSeconds <= 0){
					for(int i = 0; i < keyList.size(); i++){
						
						byte [] key = keyList.get(i).getBytes(AbstractRedisDao.DEFAULT_ENCODING);
						byte bytes [] = valueList.get(i);
						if (key.length > this.keyLenDefault || bytes.length > this.valueSizeDefault) {
							log.error ("key or value size more than default.");
							return ret;
						}
						
						pipeline.set(key, bytes);
					}
				}else{
					for(int i = 0; i < keyList.size(); i++){
						byte [] key = keyList.get(i).getBytes(AbstractRedisDao.DEFAULT_ENCODING);
						byte bytes [] = valueList.get(i);
						if (key.length > this.keyLenDefault || bytes.length > this.valueSizeDefault) {
							log.error ("key or value size more than default.");
							return ret;
						}
						
						pipeline.setex(key, liveSeconds, bytes );
					}
				}
				List<Object> returnList = pipeline.syncAndReturnAll();		
				boolean foundError = false;
				for(Object object : returnList){
					if(!AbstractRedisDao.SUCCESS_SET.equalsIgnoreCase((String)object)){
						foundError = true;
						log.warn("There is not success operation in setBytesByBatch");
						break;
					}
				}
				if(!foundError){
					ret = true;
				}
			}
		} catch (Exception e) {
	 		broken = handleException(e);
			log.error(this.getName() + " Error occurred when setBytesByBatch", e);
			ret = false;
		} finally {
			closeShardedJedis(shardedJedis, broken);
		}
		return ret;
	}

	@Override
	protected List<Object> getBytesByBatch(List<String> keyList, int index) {
		ShardedJedis shardedJedis = null;
		List<Object> returnList = null;
		boolean broken = false;		
		
		try {
			shardedJedis = this.redisPool.getResource();
			if(shardedJedis != null){
				Collection<Jedis> t = shardedJedis.getAllShards();
				for(Jedis j : t){
					j.select(index);
				}				
				ShardedJedisPipeline pipeline = shardedJedis.pipelined();
				for(String key : keyList){
					pipeline.get(key.getBytes(AbstractRedisDao.DEFAULT_ENCODING));
				}
				returnList = pipeline.syncAndReturnAll();		
			}
		} catch (Exception e) {
	 		broken = handleException(e);
			log.error(this.getName() + "Error occurred when getBytesByBatch", e);
		} finally {
			closeShardedJedis(shardedJedis, broken);
		}
		return returnList;
	}

	@Override
	protected long getKeyExpiredTime(byte[] key, int index) {
		ShardedJedis shardedJedis = null;
		long ret = 0l;
		boolean broken = false;
		try {
			shardedJedis = this.redisPool.getResource();
			if(shardedJedis != null){
				Jedis client = shardedJedis.getShard(key);
				client.select(index);
				ret = client.ttl(key);
			}
		} catch (Exception e) {
			broken = handleException(e);
			log.error(getHost(shardedJedis,key) +"  Error occurred when getKeyExpiredTime", e);
		} finally {
			closeShardedJedis(shardedJedis, broken);
		}
		return ret;
	}

	@Override
	protected boolean setHashBytes(byte[] key, byte[] field, byte[] value,int seconds, int index) {
		boolean ret = false;
		ShardedJedis shardedJedis = null;
		boolean broken = false;
		if(key == null || field == null || value == null){
			return ret;
		}
		try {
			shardedJedis = this.redisPool.getResource();
			if(shardedJedis != null){
				Jedis client = shardedJedis.getShard(key);
				client.select(index);
				long flag1 = client.hset(key, field, value);
				long flag2=1;
				if (seconds > 0 )
				    flag2 = client.expire(key, seconds);
				
				if((flag1 == 0 || flag1 == 1) && flag2 == 1){
					ret = true;
				}else{
					log.error(getHost(shardedJedis,key) +" SetHashBytes failed with key " + newString(key) + ", " + flag1 + ", " + flag2);
				}
			}
		} catch (Exception e) {
 			broken = handleException(e);
			log.error(getHost(shardedJedis,key) +"Error occurred when setHashBytes", e);
		} finally {			
			closeShardedJedis(shardedJedis, broken);
		}
		return ret;
	}

	@Override
	protected byte[] getHashBytes(byte[] key, byte[] field, int index) {
		byte[] ret = null;
		ShardedJedis shardedJedis = null;
		boolean broken = false;
		if(key == null || field == null){
			return ret;
		}
		try {
			shardedJedis = this.redisPool.getResource();
			if(shardedJedis != null){
				Jedis client = shardedJedis.getShard(key);
				client.select(index);
				ret = client.hget(key, field);
			}
		} catch (Exception e) {
 			broken = handleException(e);
			log.error(getHost(shardedJedis,key) + " Error occurred when getHashBytes", e);
		} finally {			
			closeShardedJedis(shardedJedis, broken);
		}	
		return ret;
	}

	@Override
	protected List<byte[]> getHashBytesByBatch(byte[] key, byte[][] fieldArray,int index) {
		List<byte[]> ret = null;
		ShardedJedis shardedJedis = null;
		boolean broken = false;
		if(key == null || fieldArray == null){
			return ret;
		}
		try {
			shardedJedis = this.redisPool.getResource();
			if(shardedJedis != null){
				Jedis client = shardedJedis.getShard(key);
				client.select(index);
				ret = client.hmget(key, fieldArray);
			}
		} catch (Exception e) {
 			broken = handleException(e);
			log.error(getHost(shardedJedis,key) +" Error occurred when getHashBytesByBatch", e);
		} finally {			
			closeShardedJedis(shardedJedis, broken);
		}		
		return ret;
	}

	@Override
	protected boolean setHashBytesByBatch(byte[] key, Map<byte[], byte[]> map,int seconds, int index) {
		boolean ret = false;
		ShardedJedis shardedJedis = null;
		boolean broken = false;
		if(key == null || map == null){
			return ret;
		}
		try {
			shardedJedis = this.redisPool.getResource();
			if(shardedJedis != null){
				Jedis client = shardedJedis.getShard(key);
				client.select(index);
				String flag1 = client.hmset(key, map);
				long flag2 = client.expire(key, seconds);
				if(AbstractRedisDao.SUCCESS_SET.equals(flag1) && flag2 == 1){
					ret = true;
				}else{
					log.error(getHost(shardedJedis,key) +" setHashBytesByBatch failed with key " + newString(key) + ", " + flag1 + ", " + flag2);
				}
			}
		} catch (Exception e) {
 			broken = handleException(e);
			log.error(getHost(shardedJedis,key) +" Error occurred when setHashBytesByBatch", e);
		} finally {			
			closeShardedJedis(shardedJedis, broken);
		}
		return ret;
	}

	@Override
	protected boolean delHashBytes(byte[] key, byte[] field, int index) {
		boolean ret = false;
		ShardedJedis shardedJedis = null;
		boolean broken = false;
		if(key == null || field == null){
			return ret;
		}
		try {
			shardedJedis = this.redisPool.getResource();
			if(shardedJedis != null){
				Jedis client = shardedJedis.getShard(key);
				client.select(index);
				long flag = client.hdel(key, field);
				if(flag == 1){
					ret = true;
				}else{
					log.info(" delHashBytes failed with key " + newString(key) + ", " + flag);
				}
			}
		} catch (Exception e) {
 			broken = handleException(e);
			log.error(getHost(shardedJedis,key) +" Error occurred when delHashBytes", e);
		} finally {			
			closeShardedJedis(shardedJedis, broken);
		}	
		return ret;
	}

	@Override
	protected boolean delHashBytesByBatch(byte[] key, byte[][] fieldArray,int index) {
		boolean ret = false;
		ShardedJedis shardedJedis = null;
		boolean broken = false;
		if(key == null || fieldArray == null){
			return ret;
		}
		try {
			shardedJedis = this.redisPool.getResource();
			if(shardedJedis != null){
				Jedis client = shardedJedis.getShard(key);
				client.select(index);
				long flag = client.hdel(key, fieldArray);
				if((flag <= fieldArray.length) && (flag >= 0)){
					ret = true;
				}else{
					log.info("delHashBytesByBatch failed with key " + newString(key) + ", " +flag);
				}
			}
		} catch (Exception e) {
 			broken = handleException(e);
			log.error(getHost(shardedJedis,key) +" Error occurred when delHashBytesByBatch", e);
		} finally {			
			closeShardedJedis(shardedJedis, broken);
		}	
		return ret;
	}

	@Override
	protected boolean addSetBytes(byte[] key, byte[] member, int seconds,int index) {
		boolean ret = false;
		ShardedJedis shardedJedis = null;
		boolean broken = false;
		if(key == null || member == null){
			return ret;
		}
		try {
			shardedJedis = this.redisPool.getResource();
			if(shardedJedis != null){
				Jedis client = shardedJedis.getShard(key);
				client.select(index);
				long flag1 = client.sadd(key, member);
				long flag2 = client.expire(key, seconds);
				if((flag1 == 1 || flag1 == 0) && flag2 == 1){
					ret = true;
				}else{
					log.error(getHost(shardedJedis,key) +" addSetBytes failed with key " + newString(key) + ", " + flag1 + ", " + flag2);
				}
			}
		} catch (Exception e) {
 			broken = handleException(e);
			log.error(getHost(shardedJedis,key) +" Error occurred when addSetBytes", e);
		} finally {			
			closeShardedJedis(shardedJedis, broken);
		}
		return ret;
	}

	@Override
	protected boolean addSetBytesByBatch(byte[] key, byte[][] byteArray,int seconds, int index) {
		boolean ret = false;
		ShardedJedis shardedJedis = null;
		boolean broken = false;
		if(key == null || byteArray == null){
			return ret;
		}
		try {
			shardedJedis = this.redisPool.getResource();
			if(shardedJedis != null){
				Jedis client = shardedJedis.getShard(key);
				client.select(index);
				long flag1 = client.sadd(key, byteArray);
				long flag2 = client.expire(key, seconds);
				if((flag1 <= byteArray.length) && (flag1 >= 0) && (flag2 == 1)){
					ret = true;
				}else{
					log.error(getHost(shardedJedis,key) +" addSetBytesByBatch failed with key " + newString(key) + ", " + flag1 + ", " + flag2);
				}
			}
		} catch (Exception e) {
 			broken = handleException(e);
			log.error(getHost(shardedJedis,key) +" Error occurred when addSetBytesByBatch", e);
		} finally {			
			closeShardedJedis(shardedJedis, broken);
		}
		return ret;
	}

	@Override
	protected boolean isSetByteExists(byte[] key, byte[] member, int index) {
		boolean ret = false;
		ShardedJedis shardedJedis = null;
		boolean broken = false;
		if(key == null || member == null){
			return ret;
		}
		try {
			shardedJedis = this.redisPool.getResource();
			if(shardedJedis != null){
				Jedis client = shardedJedis.getShard(key);
				client.select(index);
				ret = client.sismember(key, member);
			}
		} catch (Exception e) {
 			broken = handleException(e);
			log.error(getHost(shardedJedis,key) +" Error occurred when isSetByteExists", e);
		} finally {			
			closeShardedJedis(shardedJedis, broken);
		}	
		return ret;
	}

	@Override
	protected boolean delSetByte(byte[] key, byte[] member, int index) {
		boolean ret = false;
		ShardedJedis shardedJedis = null;
		boolean broken = false;
		if(key == null || member == null){
			return ret;
		}
		try {
			shardedJedis = this.redisPool.getResource();
			if(shardedJedis != null){
				Jedis client = shardedJedis.getShard(key);
				client.select(index);
				long flag = client.srem(key, member);
				if(flag == 1){
					ret = true;
				}else{
					log.error(getHost(shardedJedis,key) +" delSetByte failed with key " + newString(key) + ", " + flag);
				}
			}
		} catch (Exception e) {
 			broken = handleException(e);
			log.error(getHost(shardedJedis,key) +" Error occurred when delSetByte", e);
		} finally {			
			closeShardedJedis(shardedJedis, broken);
		}	
		return ret;
	}

	@Override
	protected boolean delSetBytesByBatch(byte[] key, byte[][] byteArray,int index) {
		boolean ret = false;
		ShardedJedis shardedJedis = null;
		boolean broken = false;
		if(key == null || byteArray == null){
			return ret;
		}
		try {
			shardedJedis = this.redisPool.getResource();
			if(shardedJedis != null){
				Jedis client = shardedJedis.getShard(key);
				client.select(index);
				long flag = client.srem(key, byteArray);
				if((flag <= byteArray.length) && (flag >= 0)){
					ret = true;
				}else{
					log.error(getHost(shardedJedis,key) +" delSetBytesByBatch failed with key " + newString(key) + ", " + flag);
				}
			}
		} catch (Exception e) {
 			broken = handleException(e);
			log.error(getHost(shardedJedis,key) +" Error occurred when delSetBytesByBatch", e);
		} finally {			
			closeShardedJedis(shardedJedis, broken);
		}	
		return ret;
	}

	@Override
	protected Set<byte[]> getAllSetBytes(byte[] key, int index) {
		Set<byte[]> ret = null;
		ShardedJedis shardedJedis = null;
		boolean broken = false;
		if(key == null){
			return ret;
		}
		try {
			shardedJedis = this.redisPool.getResource();
			if(shardedJedis != null){
				Jedis client = shardedJedis.getShard(key);
				client.select(index);
				ret = client.smembers(key);
			}
		} catch (Exception e) {
 			broken = handleException(e);
			log.error(getHost(shardedJedis,key) +" Error occurred when getAllSetBytes", e);
		} finally {			
			closeShardedJedis(shardedJedis, broken);
		}	
		return ret;
	}

	@Override
	protected boolean addSortedSetByte(byte[] key, byte[] member, double score,int seconds, int index) {
		boolean ret = false;
		ShardedJedis shardedJedis = null;
		boolean broken = false;
		if(key == null || member == null){
			return ret;
		}
		try {
			shardedJedis = this.redisPool.getResource();
			if(shardedJedis != null){
				Jedis client = shardedJedis.getShard(key);
				client.select(index);
				long flag1 = client.zadd(key, score, member);
				long flag2 = client.expire(key, seconds);
				if((flag1 == 1 || flag1 == 0) && flag2 == 1){
					ret = true;
				}else{
					log.error(getHost(shardedJedis,key) +" addSortedSetByte failed with key " + newString(key) + ", " + flag1 + ", " + flag2);
				}
			}
		} catch (Exception e) {
 			broken = handleException(e);
			log.error(getHost(shardedJedis,key) +" Error occurred when addSortedSetByte", e);
		} finally {			
			closeShardedJedis(shardedJedis, broken);
		}
		return ret;
	}

	@Override
	protected boolean addSortedSetByteByBatch(byte[] key,Map<byte[], Double> memberBytes, int seconds, int index) {
		boolean ret = false;
		ShardedJedis shardedJedis = null;
		boolean broken = false;
		if(key == null || memberBytes == null){
			return ret;
		}
		try {
			shardedJedis = this.redisPool.getResource();
			if(shardedJedis != null){
				Jedis client = shardedJedis.getShard(key);
				client.select(index);
				long flag1 = client.zadd(key, memberBytes);
				long flag2 = client.expire(key, seconds);
				if((flag1 <= memberBytes.size()) && (flag1 >= 0) && (flag2 == 1)){
					ret = true;
				}else{
					log.error(getHost(shardedJedis,key) +" addSortedSetByteByBatch failed with key " + newString(key) + ", " + flag1 + ", " + flag2);
				}
			}
		} catch (Exception e) {
 			broken = handleException(e);
			log.error(getHost(shardedJedis,key) +" Error occurred when addSortedSetByteByBatch", e);
		} finally {			
			closeShardedJedis(shardedJedis, broken);
		}
		return ret;
	}

	@Override
	public long getSortedSetSize(String key, int index) {
		long ret = -1;
		ShardedJedis shardedJedis = null;
		boolean broken = false;
		if(key == null){
			return ret;
		}
		try {
			shardedJedis = this.redisPool.getResource();
			if(shardedJedis != null){
				Jedis client = shardedJedis.getShard(key);
				client.select(index);
				ret = client.zcard(key);
			}
		} catch (Exception e) {
 			broken = handleException(e);
			log.error(getHost(shardedJedis,key) +" Error occurred when getSortedSetSize", e);
		} finally {			
			closeShardedJedis(shardedJedis, broken);
		}	
		return ret;
	}

	@Override
	protected double incrSortedSetByteScore(byte[] key, byte[] member,double score, int index) {
		double ret = 0;
		ShardedJedis shardedJedis = null;
		boolean broken = false;
		if(key == null || member == null){
			return ret;
		}
		try {
			shardedJedis = this.redisPool.getResource();
			if(shardedJedis != null){
				Jedis client = shardedJedis.getShard(key);
				client.select(index);
				Double retValue = client.zincrby(key, score, member);
				if(retValue != null){
					ret = retValue.doubleValue();
				}
			}
		} catch (Exception e) {
 			broken = handleException(e);
			log.error(getHost(shardedJedis,key) +" Error occurred when incrSortedSetByteScore", e);
		} finally {			
			closeShardedJedis(shardedJedis, broken);
		}
		return ret;
	}

	@Override
	protected Set<byte[]> getSortedSetByteRange(byte[] key, long start,long end, int index) {
		Set<byte[]> ret = null;
		ShardedJedis shardedJedis = null;
		boolean broken = false;
		if(key == null){
			return ret;
		}
		try {
			shardedJedis = this.redisPool.getResource();
			if(shardedJedis != null){
				Jedis client = shardedJedis.getShard(key);
				client.select(index);
				ret = client.zrange(key, start, end);
			}
		} catch (Exception e) {
 			broken = handleException(e);
			log.error(getHost(shardedJedis,key) +" Error occurred when getSortedSetByteRange", e);
		} finally {			
			closeShardedJedis(shardedJedis, broken);
		}	
		return ret;
	}

	@Override
	protected Set<byte[]> getSortedSetByteRevRange(byte[] key, long start,long end, int index) {
		Set<byte[]> ret = null;
		ShardedJedis shardedJedis = null;
		boolean broken = false;
		if(key == null){
			return ret;
		}
		try {
			shardedJedis = this.redisPool.getResource();
			if(shardedJedis != null){
				Jedis client = shardedJedis.getShard(key);
				client.select(index);
				ret = client.zrevrange(key, start, end);
			}
		} catch (Exception e) {
 			broken = handleException(e);
			log.error(getHost(shardedJedis,key) +" Error occurred when getSortedSetByteRevRange", e);
		} finally {			
			closeShardedJedis(shardedJedis, broken);
		}	
		return ret;
	}

	@Override
	protected boolean delSortedSetByte(byte[] key, byte[] member, int index) {
		boolean ret = false;
		ShardedJedis shardedJedis = null;
		boolean broken = false;
		if(key == null || member == null){
			return ret;
		}
		try {
			shardedJedis = this.redisPool.getResource();
			if(shardedJedis != null){
				Jedis client = shardedJedis.getShard(key);
				client.select(index);
				long flag = client.zrem(key, member);
				if(flag == 1){
					ret = true;
				}else{
					log.error(getHost(shardedJedis,key) +" delSortedSetByte failed with key " + newString(key) + ", " + flag);
				}
			}
		} catch (Exception e) {
 			broken = handleException(e);
			log.error(getHost(shardedJedis,key) +" Error occurred when delSortedSetByte", e);
		} finally {			
			closeShardedJedis(shardedJedis, broken);
		}	
		return ret;
	}

	@Override
	protected boolean delSortedSetBytesByBatch(byte[] key, byte[][] byteArray,int index) {
		boolean ret = false;
		ShardedJedis shardedJedis = null;
		boolean broken = false;
		if(key == null || byteArray == null){
			return ret;
		}
		try {
			shardedJedis = this.redisPool.getResource();
			if(shardedJedis != null){
				Jedis client = shardedJedis.getShard(key);
				client.select(index);
				long flag = client.zrem(key, byteArray);
				if((flag <= byteArray.length) && (flag >= 0)){
					ret = true;
				}else{
					log.error(getHost(shardedJedis,key) +" delSortedSetBytesByBatch failed with key " + newString(key) + ", " + flag);
				}
			}
		} catch (Exception e) {
 			broken = handleException(e);
			log.error(getHost(shardedJedis,key) +" Error occurred when delSortedSetBytesByBatch", e);
		} finally {			
			closeShardedJedis(shardedJedis, broken);
		}	
		return ret;
	}

	@Override
	public boolean flushAll(String[] servers) {
		ShardedJedis shardedJedis = null;
		boolean broken = false;
		try {
		if (servers != null) {
			ShardedJedis t = redisPool.getResource();
			s:
			for (JedisShardInfo j: t.getAllShardInfo()) {
				String ht=j.getHost()+":"+j.getPort();
				for (String h:servers) {
					if (ht.equals(h)) {
						shardedJedis =t;
						break s;
					}
				}
			}
		}  else {
			shardedJedis = redisPool.getResource();
		}
		
		if (shardedJedis != null) {
			for ( Jedis c:shardedJedis.getAllShards()) {
				 c.flushAll();
			}
			
			return true;
		}
		} catch (Exception e) {
 			broken = handleException(e);
		} finally {			
			closeShardedJedis(shardedJedis, broken);
		}	
		
		return false;
	}

	
}
