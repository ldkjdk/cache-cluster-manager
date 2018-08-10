package com.dhgate.redis;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dhgate.ICachedConfigManager;
import com.dhgate.SafeEncoder;
import com.dhgate.redis.clients.jedis.Jedis;
import com.dhgate.redis.clients.jedis.JedisPoolConfig;
import com.dhgate.redis.clients.jedis.JedisPubSub;
import com.dhgate.redis.clients.jedis.JedisSentinelPool;
import com.dhgate.redis.clients.jedis.Pipeline;
import com.dhgate.redis.clients.jedis.exceptions.JedisConnectionException;

/**
 * 
 * @author lidingkun
 *
 */

public class SentinelRedisCache extends  AbstractRedisDao{

	private static final Logger log = LoggerFactory.getLogger(SentinelRedisCache.class);
	
	
	private ICachedConfigManager cacheManager;
	private JedisSentinelPool  redisPool = null;
	private String name;
	private volatile int keyLenDefault = AbstractRedisDao.KEY_LEN_DEFAULT; //byte 
	private volatile int valueSizeDefault = AbstractRedisDao.VALUE_SIZE_DEFAULT; //byte
	private String servers[];
	private int weights [];
	private JedisPoolConfig config;
	private int timeout;
	private int weight =1;
	
	private String masterName=null;
	
	public SentinelRedisCache (String name,String[] servers,String masterName, JedisPoolConfig config, int timeout) {
		super();
		this.name = name;
		this.servers = servers;
		this.config = config;
		this.timeout = timeout;
		this.masterName=masterName;
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
		Jedis client = null;
		boolean broken = false;
		try {
			client = redisPool.getResource();
			if (client != null) {
				client.select(0);
				ret = client.del(key);
			}

		} catch (Exception e) {
			broken = handleException(e);
			log.error(this.getName() +" Error occurred when delFromPool", e);
			ret = -1;
		} finally {
			this.closeJedis(client, broken);
		}

		return ret > 0;
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
		Jedis client = null;
		boolean ret = false;
		/** flag indicating whether the connection needs to be dropped or not */
		boolean broken = false;
		try {

			client = redisPool.getResource();
			if (client != null) {
				client.select(index);
				ret = client.exists(key);
			}

		} catch (Exception e) {
			broken = handleException(e);
			log.error(this.getName() +" Error occurred when isExist", e);
		} finally {
			this.closeJedis(client, broken);
		}
		return ret;
	}
	
	private List<Object> getBytesByBatch(String [] keyList) {
		
		Jedis client = null;
		List<Object> returnList = null;
		boolean broken = false;		
		try {
			client = redisPool.getResource();
			if(client != null){		
				Pipeline pipeline = client.pipelined();
				for(String key : keyList){
					pipeline.get(key.getBytes(AbstractRedisDao.DEFAULT_ENCODING));
				}
				returnList = pipeline.syncAndReturnAll();		
			}
		} catch (Exception e) {
	 		broken = handleException(e);
			log.error(this.getName() +" Error occurred when getBytesByBatch", e);
		} finally {
			closeJedis(client, broken);
		}
		return returnList;
	}
	@Override
	public byte[] getBytes(byte[] key, int index) {
		if (key == null)
			return null;
		Jedis client = null;
		byte[] ret = null;
		/** flag indicating whether the connection needs to be dropped or not */
		 boolean broken = false;
			try {
				
				client = redisPool.getResource();	
				client.select(index);
				ret = client.get(key);
				return ret;
			} catch (Exception e) {
	 			broken = handleException(e);
	 			log.error(this.getName() +" Error occurred when getBytes from slave", e);
			} finally {
				closeJedis(client,broken);
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
		
		Jedis client = null;
		String ret = null;
		
		/** flag indicating whether the connection needs to be dropped or not */
		 boolean broken = false;
		try {
			client = redisPool.getResource();
			if(client != null){
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
 			log.error(this.getName() +" Error occurred when setBytes", e);
		} finally {
			closeJedis(client, broken);
		}
		
		return (AbstractRedisDao.SUCCESS_SET.equals(ret));
	}
	
	private void closeJedis(Jedis jedis, boolean broken) {
		if (jedis != null) {
			if (!broken) {
				redisPool.returnResource(jedis);
			} else {
				redisPool.returnBrokenResource(jedis);
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

	public JedisSentinelPool getRedisPool() {
		return redisPool;
	}

	public void setRedisPool(JedisSentinelPool redisPool) {
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
		
		Set<String> sentinelSet = new HashSet<String>();
		for (String str: servers) {		
			sentinelSet.add(str);
		}
		
		this.redisPool = new JedisSentinelPool(masterName, sentinelSet, this.config, timeout, null, 0);
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
		    	JedisSentinelPool tempPool= this.redisPool;
		    	this.servers=servers;
		    	//this.hashingAlg=hashingAlg;
		    	//this.weights=weights;
		    	this.config=conf;
		    	
				this.init();
				tempPool.close();
				
				log.info("++++finished updatePool for name " + this.name);
		    } else if (isDifferent(conf)) {
		    	this.config=conf;
		    	this.redisPool.setConfig(this.config);
		    } 
		} catch (Exception e) {
			log.error(this.getName() +" initialize failed ",e);
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
		Jedis sj = redisPool.getResource();
		try {
			if(AbstractRedisDao.PING_RESPONSE.equals(sj.ping())){
				return true;
			}
		
		}catch (Exception e) {
 			broken = handleException(e);
 			log.error(this.getName() +" Error occurred when getBytes from slave", e);
		} finally {
			this.closeJedis(sj,broken);
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
		Jedis client = null;
		String ret = null;
		/** flag indicating whether the connection needs to be dropped or not */
		 boolean broken = false;
		try {
			client = this.redisPool.getResource();
			if(client != null){
				
				client.select(index);
				if (liveSeconds <= 0) {
					ret = client.set(key, str);
				}else{
					ret = client.setex(key, liveSeconds, str);
				}
			}
		} catch (Exception e) {
 			broken = handleException(e);
			log.error(this.getName() +" Error occurred when setString", e);		
		} finally {
			closeJedis(client, broken);
		}
		return ret;	}

	@Override
	public String getString(String key, int index) {
		Jedis client = null;
		String ret = null;
		/** flag indicating whether the connection needs to be dropped or not */
		boolean broken = false;
		
			try {
				client = redisPool.getResource();	
				client.select(index);
				ret = client.get(key);
				return ret;
			} catch (Exception e) {
	 			broken = handleException(e);
				log.error(this.getName() +" Error occurred when getString",e);
			} finally {
				closeJedis(client, broken);
			}
		return ret;
	}

	@Override
	public boolean delKeyByBatch(List<String> keyList, int index) {
		Jedis client = null;
		boolean ret = false;
		boolean broken = false;		
		
		try {
			client = redisPool.getResource();
			if(client != null){
				client.select(index);			
				Pipeline pipeline = client.pipelined();
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
			closeJedis(client, broken);
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
		
         Object result = null;
		 boolean broken = false;
		
		 Jedis client = null;
			
			try {
				client = this.redisPool.getResource();
				if(client != null){
					result=client.eval(script,keys,args);
				}
			} catch (Exception e) {
	 			broken = handleException(e);
				log.error(this.getName() +" Error occurred when lock", e);
				throw e;
			} finally {
				closeJedis(client, broken);
			}
			return result;
	}	

	@Override
	public long incr(String key, int index) {
		if (key == null){
			return 0L;
		}
		Jedis client = null;
		long ret = 0L;
		/** flag indicating whether the connection needs to be dropped or not */
		boolean broken = false;
		try {
			client =this.redisPool.getResource();			
			if(client != null){
				client.select(index);
				ret = client.incr(key);
			}
		} catch (Exception e) {
 			broken = handleException(e);
			log.error(this.getName() +" Error occurred when incr", e);
			ret= -1;		
		} finally {
			closeJedis(client, broken);
		}
		return ret;
	}

	@Override
	public long incrBy(String key, long integer,int index) {
		if (key == null){
			return 0L;
		}
		Jedis client = null;
		long ret = 0L;
		/** flag indicating whether the connection needs to be dropped or not */
		boolean broken = false;
		try {
			client =this.redisPool.getResource();			
			if(client != null){
				client.select(index);
				ret = client.incrBy(key, integer);
			}
		} catch (Exception e) {
 			broken = handleException(e);
			log.error(this.getName() +" Error occurred when incr", e);
			ret= -1;		
		} finally {
			closeJedis(client, broken);
		}
		return ret;
	}
	
	@Override
	public long decr(String key, int index) {
		if (key == null){
			return 0L;
		}
		Jedis client = null;
		long ret = 0L;
		/** flag indicating whether the connection needs to be dropped or not */
		boolean broken = false;
		try {
			client =this.redisPool.getResource();			
			if(client != null){
				client.select(index);
				ret = client.decr(key);
			}
		} catch (Exception e) {
 			broken = handleException(e);
			log.error(this.getName() +" Error occurred when incr", e);
			ret= -1;		
		} finally {
			closeJedis(client, broken);
		}
		return ret;
	}
	
	
	@Override
	public long decrBy(String key, long integer,int index) {
		if (key == null){
			return 0L;
		}
		Jedis client = null;
		long ret = 0L;
		/** flag indicating whether the connection needs to be dropped or not */
		boolean broken = false;
		try {
			client =this.redisPool.getResource();			
			if(client != null){
				client.select(index);
				ret = client.decrBy(key, integer);
			}
		} catch (Exception e) {
 			broken = handleException(e);
			log.error(this.getName() +" Error occurred when incr", e);
			ret= -1;		
		} finally {
			closeJedis(client, broken);
		}
		return ret;
	}
	
	@Override
	public long expire(String key, int seconds, int index) {
		if (key == null) {
			return 0L;
		}
		Jedis client = null;
		long ret = 0L;
		boolean broken = false;
		try {
			client = this.redisPool.getResource();
			if(client != null){
				
				client.select(index);
				ret = client.expire(key, seconds);
			}
		} catch (Exception e) {
 			broken = handleException(e);
			log.error(this.getName() +" Error occurred when expire", e);
			ret= -1;
		} finally {		
			closeJedis(client, broken);
		}
		return ret;	
	}
	
	

	@Override
	public long expireAt(String key, long unixTime, int index) {
		if(key == null) {
			return 0;
		}
		
		long ret = 0L;
		Jedis client = null;
		boolean broken = false;
		try {
			client = this.redisPool.getResource();
			if(client != null){
				
				client.select(index);
				ret = client.expireAt(key, unixTime);	
			}
		} catch (Exception e) {
 			broken = handleException(e);
			log.error(this.getName() +" Error occurred when expireAt", e);
			ret= -1;
		} finally {			
			closeJedis(client, broken);
		}
		return ret;
	}

	@Override
	public long getHashLength(String key, int index) {
		long ret = -1;
		Jedis client = null;
		boolean broken = false;
		if(key == null){
			return ret;
		}
		try {
			client = this.redisPool.getResource();
			if(client != null){
				
				client.select(index);
				ret = client.hlen(key);
			}
		} catch (Exception e) {
 			broken = handleException(e);
			log.error(this.getName() +" Error occurred when getHashLength", e);
		} finally {			
			closeJedis(client, broken);
		}	
		return ret;
	}

	@Override
	public boolean isHashFieldExist(String key, String field, int index) {
		boolean ret = false;
		Jedis client = null;
		boolean broken = false;
		if(key == null){
			return ret;
		}
		try {
			client = this.redisPool.getResource();
			if(client != null){
				
				client.select(index);
				ret = client.hexists(key, field);
			}
		} catch (Exception e) {
 			broken = handleException(e);
			log.error(this.getName() +" Error occurred when isHashFieldExist", e);
		} finally {			
			closeJedis(client, broken);
		}		
		return ret;
	}

	@Override
	public long getSetSize(String key, int index) {
		long ret = -1;
		Jedis client = null;
		boolean broken = false;
		if(key == null){
			return ret;
		}
		try {
			client = this.redisPool.getResource();
			if(client != null){
				
				client.select(index);
				ret = client.scard(key);
			}
		} catch (Exception e) {
 			broken = handleException(e);
			log.error(this.getName() +" Error occurred when getSetSize", e);
		} finally {			
			closeJedis(client, broken);
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
        Jedis client = null;
        Long ret = null;
        /** flag indicating whether the connection needs to be dropped or not */
         boolean broken = false;
        try {
            client = this.redisPool.getResource();
            if(client != null){
                
               // client.select(0);
                 ret = client.publish(key, message);
                
            }
        } catch (Exception e) {
            broken = handleException(e);
            log.error(this.getName() +" Error occurred when setString", e);      
        } finally {
            closeJedis(client, broken);
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
        Jedis client = null;
        
        /** flag indicating whether the connection needs to be dropped or not */
         boolean broken = false;
        try {
            client = this.redisPool.getResource();
            if(client != null){
                
               // client.select(0);
               
                client.subscribe(jedisPubSub, key);
                
            }
        } catch (Exception e) {
            broken = handleException(e);
            log.error(this.getName() +" Error occurred when setString", e);      
        } finally {
            closeJedis(client, broken);
        }
		
	}

	@Override
	public Map<String, String> hgetAll(String key, int index) {
		 Jedis client = null;
	        Map<String,String> ret = null;
	        /** flag indicating whether the connection needs to be dropped or not */
	        boolean broken = false;
		try {
            client = this.redisPool.getResource();
            if(client != null){
                //log.info ("TEST :"+key +" ="+client.toString()); for test
                client.select(index);
                ret = client.hgetAll(key);
            }
        } catch (Exception e) {
            broken = handleException(e);
            log.error(this.getName() +" Error occurred when getStringFromPool",e);
        } finally {
            closeJedis(client, broken);
        }       
       
		return ret;
	}

	@Override
	public Map<String, Object> getHashAllObject(String key, int index) {
		 Jedis client = null;
	        Map<byte[],byte []> ret = null;
	        /** flag indicating whether the connection needs to be dropped or not */
	        boolean broken = false;
	        try {
	            client = this.redisPool.getResource();
	            if(client != null){
	                //log.info ("TEST :"+key +" ="+client.toString()); for test
	                client.select(index);
	                ret = client.hgetAll(key.getBytes(AbstractRedisDao.DEFAULT_ENCODING));
	            }
	        } catch (Exception e) {
	            broken = handleException(e);
	            log.error(this.getName() +" Error occurred when getStringFromPool",e);
	        } finally {
	            closeJedis(client, broken);
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
		
		Jedis client = null;
		String ret = null;
		/** flag indicating whether the connection needs to be dropped or not */
		 boolean broken = false;
		try {
			client = this.redisPool.getResource();
			if(client != null){
				client.select(index);
				if (seconds <= 0) {
					ret = client.set(key, bytes);
				} else {
					ret = client.setex(key, seconds, bytes);
				}
			}
		} catch (Exception e) {
 			broken = handleException(e);
 			log.error(this.getName() +" Error occurred when setBytes", e);
		} finally {
			closeJedis(client, broken);
		}
		return ret;
	}

	@Override
	protected long del(String key, int index) {
		if (key == null){
			return 0L;
		}
		long ret = 0L;		
		Jedis client = null;			
		boolean broken = false;
		try {
			client = this.redisPool.getResource();
			if(client != null){
				client.select(index);
				ret = client.del(key);
			}
		} catch (Exception e) {
 			broken = handleException(e);
			log.error(this.getName() +" Error occurred when delFromPool", e);
			ret= -1;
		} finally {
			closeJedis(client, broken);
		}
		return ret;
	}

	@Override
	protected boolean setBytesByBatch(List<String> keyList,List<byte[]> valueList, int liveSeconds, int index) {
		Jedis client = null;
		boolean ret = false;
		boolean broken = false;		
		
		try {
			client = this.redisPool.getResource();
			if(client != null){	
				client.select(index);
				Pipeline pipeline = client.pipelined();
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
			log.error(this.getName() +" Error occurred when setBytesByBatch", e);
			ret = false;
		} finally {
			closeJedis(client, broken);
		}
		return ret;
	}

	@Override
	protected List<Object> getBytesByBatch(List<String> keyList, int index) {
		Jedis client = null;
		List<Object> returnList = null;
		boolean broken = false;		
		
		try {
			client = this.redisPool.getResource();
			if(client != null){
				client.select(index);			
				Pipeline pipeline = client.pipelined();
				for(String key : keyList){
					pipeline.get(key.getBytes(AbstractRedisDao.DEFAULT_ENCODING));
				}
				returnList = pipeline.syncAndReturnAll();		
			}
		} catch (Exception e) {
	 		broken = handleException(e);
			log.error(this.getName() +" Error occurred when getBytesByBatch", e);
		} finally {
			closeJedis(client, broken);
		}
		return returnList;
	}

	@Override
	protected long getKeyExpiredTime(byte[] key, int index) {
		Jedis client = null;
		long ret = 0l;
		boolean broken = false;
		try {
			client = this.redisPool.getResource();
			if(client != null){
				
				client.select(index);
				ret = client.ttl(key);
			}
		} catch (Exception e) {
			broken = handleException(e);
			log.error(this.getName() +" Error occurred when getKeyExpiredTime", e);
		} finally {
			closeJedis(client, broken);
		}
		return ret;
	}

	@Override
	protected boolean setHashBytes(byte[] key, byte[] field, byte[] value,int seconds, int index) {
		boolean ret = false;
		Jedis client = null;
		boolean broken = false;
		if(key == null || field == null || value == null){
			return ret;
		}
		try {
			client = this.redisPool.getResource();
			if(client != null){
				
				client.select(index);
				long flag1 = client.hset(key, field, value);
				long flag2=1;
				if (seconds > 0 )
				    flag2 = client.expire(key, seconds);
				
				if((flag1 == 0 || flag1 == 1) && flag2 == 1){
					ret = true;
				}else{
					log.error(this.getName() +" SetHashBytes failed with key " + newString(key) + ", " + flag1 + ", " + flag2);
				}
			}
		} catch (Exception e) {
 			broken = handleException(e);
			log.error(this.getName() +" Error occurred when setHashBytes", e);
		} finally {			
			closeJedis(client, broken);
		}
		return ret;
	}

	@Override
	protected byte[] getHashBytes(byte[] key, byte[] field, int index) {
		byte[] ret = null;
		Jedis client = null;
		boolean broken = false;
		if(key == null || field == null){
			return ret;
		}
		try {
			client = this.redisPool.getResource();
			if(client != null){
				
				client.select(index);
				ret = client.hget(key, field);
			}
		} catch (Exception e) {
 			broken = handleException(e);
			log.error(this.getName() +" Error occurred when getHashBytes", e);
		} finally {			
			closeJedis(client, broken);
		}	
		return ret;
	}

	@Override
	protected List<byte[]> getHashBytesByBatch(byte[] key, byte[][] fieldArray,int index) {
		List<byte[]> ret = null;
		Jedis client = null;
		boolean broken = false;
		if(key == null || fieldArray == null){
			return ret;
		}
		try {
			client = this.redisPool.getResource();
			if(client != null){
				
				client.select(index);
				ret = client.hmget(key, fieldArray);
			}
		} catch (Exception e) {
 			broken = handleException(e);
			log.error(this.getName() +" Error occurred when getHashBytesByBatch", e);
		} finally {			
			closeJedis(client, broken);
		}		
		return ret;
	}

	@Override
	protected boolean setHashBytesByBatch(byte[] key, Map<byte[], byte[]> map,int seconds, int index) {
		boolean ret = false;
		Jedis client = null;
		boolean broken = false;
		if(key == null || map == null){
			return ret;
		}
		try {
			client = this.redisPool.getResource();
			if(client != null){
				
				client.select(index);
				String flag1 = client.hmset(key, map);
				long flag2 = client.expire(key, seconds);
				if(AbstractRedisDao.SUCCESS_SET.equals(flag1) && flag2 == 1){
					ret = true;
				}else{
					log.error(this.getName() +" setHashBytesByBatch failed with key " + newString(key) + ", " + flag1 + ", " + flag2);
				}
			}
		} catch (Exception e) {
 			broken = handleException(e);
			log.error(this.getName() +" Error occurred when setHashBytesByBatch", e);
		} finally {			
			closeJedis(client, broken);
		}
		return ret;
	}

	@Override
	protected boolean delHashBytes(byte[] key, byte[] field, int index) {
		boolean ret = false;
		Jedis client = null;
		boolean broken = false;
		if(key == null || field == null){
			return ret;
		}
		try {
			client = this.redisPool.getResource();
			if(client != null){
				
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
			log.error(this.getName() +" Error occurred when delHashBytes", e);
		} finally {			
			closeJedis(client, broken);
		}	
		return ret;
	}

	@Override
	protected boolean delHashBytesByBatch(byte[] key, byte[][] fieldArray,int index) {
		boolean ret = false;
		Jedis client = null;
		boolean broken = false;
		if(key == null || fieldArray == null){
			return ret;
		}
		try {
			client = this.redisPool.getResource();
			if(client != null){
				
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
			log.error(this.getName() +" Error occurred when delHashBytesByBatch", e);
		} finally {			
			closeJedis(client, broken);
		}	
		return ret;
	}

	@Override
	protected boolean addSetBytes(byte[] key, byte[] member, int seconds,int index) {
		boolean ret = false;
		Jedis client = null;
		boolean broken = false;
		if(key == null || member == null){
			return ret;
		}
		try {
			client = this.redisPool.getResource();
			if(client != null){
				
				client.select(index);
				long flag1 = client.sadd(key, member);
				long flag2 = client.expire(key, seconds);
				if((flag1 == 1 || flag1 == 0) && flag2 == 1){
					ret = true;
				}else{
					log.error(this.getName() +" addSetBytes failed with key " + newString(key) + ", " + flag1 + ", " + flag2);
				}
			}
		} catch (Exception e) {
 			broken = handleException(e);
			log.error(this.getName() +" Error occurred when addSetBytes", e);
		} finally {			
			closeJedis(client, broken);
		}
		return ret;
	}

	@Override
	protected boolean addSetBytesByBatch(byte[] key, byte[][] byteArray,int seconds, int index) {
		boolean ret = false;
		Jedis client = null;
		boolean broken = false;
		if(key == null || byteArray == null){
			return ret;
		}
		try {
			client = this.redisPool.getResource();
			if(client != null){
				
				client.select(index);
				long flag1 = client.sadd(key, byteArray);
				long flag2 = client.expire(key, seconds);
				if((flag1 <= byteArray.length) && (flag1 >= 0) && (flag2 == 1)){
					ret = true;
				}else{
					log.error(this.getName() +" addSetBytesByBatch failed with key " + newString(key) + ", " + flag1 + ", " + flag2);
				}
			}
		} catch (Exception e) {
 			broken = handleException(e);
			log.error(this.getName() +" Error occurred when addSetBytesByBatch", e);
		} finally {			
			closeJedis(client, broken);
		}
		return ret;
	}

	@Override
	protected boolean isSetByteExists(byte[] key, byte[] member, int index) {
		boolean ret = false;
		Jedis client = null;
		boolean broken = false;
		if(key == null || member == null){
			return ret;
		}
		try {
			client = this.redisPool.getResource();
			if(client != null){
				
				client.select(index);
				ret = client.sismember(key, member);
			}
		} catch (Exception e) {
 			broken = handleException(e);
			log.error(this.getName() +" Error occurred when isSetByteExists", e);
		} finally {			
			closeJedis(client, broken);
		}	
		return ret;
	}

	@Override
	protected boolean delSetByte(byte[] key, byte[] member, int index) {
		boolean ret = false;
		Jedis client = null;
		boolean broken = false;
		if(key == null || member == null){
			return ret;
		}
		try {
			client = this.redisPool.getResource();
			if(client != null){
				
				client.select(index);
				long flag = client.srem(key, member);
				if(flag == 1){
					ret = true;
				}else{
					log.error(this.getName() +" delSetByte failed with key " + newString(key) + ", " + flag);
				}
			}
		} catch (Exception e) {
 			broken = handleException(e);
			log.error(this.getName() +" Error occurred when delSetByte", e);
		} finally {			
			closeJedis(client, broken);
		}	
		return ret;
	}

	@Override
	protected boolean delSetBytesByBatch(byte[] key, byte[][] byteArray,int index) {
		boolean ret = false;
		Jedis client = null;
		boolean broken = false;
		if(key == null || byteArray == null){
			return ret;
		}
		try {
			client = this.redisPool.getResource();
			if(client != null){
				
				client.select(index);
				long flag = client.srem(key, byteArray);
				if((flag <= byteArray.length) && (flag >= 0)){
					ret = true;
				}else{
					log.error(this.getName() +" delSetBytesByBatch failed with key " + newString(key) + ", " + flag);
				}
			}
		} catch (Exception e) {
 			broken = handleException(e);
			log.error(this.getName() +" Error occurred when delSetBytesByBatch", e);
		} finally {			
			closeJedis(client, broken);
		}	
		return ret;
	}

	@Override
	protected Set<byte[]> getAllSetBytes(byte[] key, int index) {
		Set<byte[]> ret = null;
		Jedis client = null;
		boolean broken = false;
		if(key == null){
			return ret;
		}
		try {
			client = this.redisPool.getResource();
			if(client != null){
				
				client.select(index);
				ret = client.smembers(key);
			}
		} catch (Exception e) {
 			broken = handleException(e);
			log.error(this.getName() +" Error occurred when getAllSetBytes", e);
		} finally {			
			closeJedis(client, broken);
		}	
		return ret;
	}

	@Override
	protected boolean addSortedSetByte(byte[] key, byte[] member, double score,int seconds, int index) {
		boolean ret = false;
		Jedis client = null;
		boolean broken = false;
		if(key == null || member == null){
			return ret;
		}
		try {
			client = this.redisPool.getResource();
			if(client != null){
				
				client.select(index);
				long flag1 = client.zadd(key, score, member);
				long flag2 = client.expire(key, seconds);
				if((flag1 == 1 || flag1 == 0) && flag2 == 1){
					ret = true;
				}else{
					log.error(this.getName() +" addSortedSetByte failed with key " + newString(key) + ", " + flag1 + ", " + flag2);
				}
			}
		} catch (Exception e) {
 			broken = handleException(e);
			log.error(this.getName() +" Error occurred when addSortedSetByte", e);
		} finally {			
			closeJedis(client, broken);
		}
		return ret;
	}

	@Override
	protected boolean addSortedSetByteByBatch(byte[] key,Map<byte[], Double> memberBytes, int seconds, int index) {
		boolean ret = false;
		Jedis client = null;
		boolean broken = false;
		if(key == null || memberBytes == null){
			return ret;
		}
		try {
			client = this.redisPool.getResource();
			if(client != null){
				
				client.select(index);
				long flag1 = client.zadd(key, memberBytes);
				long flag2 = client.expire(key, seconds);
				if((flag1 <= memberBytes.size()) && (flag1 >= 0) && (flag2 == 1)){
					ret = true;
				}else{
					log.error(this.getName() +" addSortedSetByteByBatch failed with key " + newString(key) + ", " + flag1 + ", " + flag2);
				}
			}
		} catch (Exception e) {
 			broken = handleException(e);
			log.error(this.getName() +" Error occurred when addSortedSetByteByBatch", e);
		} finally {			
			closeJedis(client, broken);
		}
		return ret;
	}

	@Override
	public long getSortedSetSize(String key, int index) {
		long ret = -1;
		Jedis client = null;
		boolean broken = false;
		if(key == null){
			return ret;
		}
		try {
			client = this.redisPool.getResource();
			if(client != null){
				
				client.select(index);
				ret = client.zcard(key);
			}
		} catch (Exception e) {
 			broken = handleException(e);
			log.error(this.getName() +" Error occurred when getSortedSetSize", e);
		} finally {			
			closeJedis(client, broken);
		}	
		return ret;
	}

	@Override
	protected double incrSortedSetByteScore(byte[] key, byte[] member,double score, int index) {
		double ret = 0;
		Jedis client = null;
		boolean broken = false;
		if(key == null || member == null){
			return ret;
		}
		try {
			client = this.redisPool.getResource();
			if(client != null){
				
				client.select(index);
				Double retValue = client.zincrby(key, score, member);
				if(retValue != null){
					ret = retValue.doubleValue();
				}
			}
		} catch (Exception e) {
 			broken = handleException(e);
			log.error(this.getName() +" Error occurred when incrSortedSetByteScore", e);
		} finally {			
			closeJedis(client, broken);
		}
		return ret;
	}

	@Override
	protected Set<byte[]> getSortedSetByteRange(byte[] key, long start,long end, int index) {
		Set<byte[]> ret = null;
		Jedis client = null;
		boolean broken = false;
		if(key == null){
			return ret;
		}
		try {
			client = this.redisPool.getResource();
			if(client != null){
				
				client.select(index);
				ret = client.zrange(key, start, end);
			}
		} catch (Exception e) {
 			broken = handleException(e);
			log.error(this.getName() +" Error occurred when getSortedSetByteRange", e);
		} finally {			
			closeJedis(client, broken);
		}	
		return ret;
	}

	@Override
	protected Set<byte[]> getSortedSetByteRevRange(byte[] key, long start,long end, int index) {
		Set<byte[]> ret = null;
		Jedis client = null;
		boolean broken = false;
		if(key == null){
			return ret;
		}
		try {
			client = this.redisPool.getResource();
			if(client != null){
				
				client.select(index);
				ret = client.zrevrange(key, start, end);
			}
		} catch (Exception e) {
 			broken = handleException(e);
			log.error(this.getName() +" Error occurred when getSortedSetByteRevRange", e);
		} finally {			
			closeJedis(client, broken);
		}	
		return ret;
	}

	@Override
	protected boolean delSortedSetByte(byte[] key, byte[] member, int index) {
		boolean ret = false;
		Jedis client = null;
		boolean broken = false;
		if(key == null || member == null){
			return ret;
		}
		try {
			client = this.redisPool.getResource();
			if(client != null){
				
				client.select(index);
				long flag = client.zrem(key, member);
				if(flag == 1){
					ret = true;
				}else{
					log.error(this.getName() +" delSortedSetByte failed with key " + newString(key) + ", " + flag);
				}
			}
		} catch (Exception e) {
 			broken = handleException(e);
			log.error(this.getName() +" Error occurred when delSortedSetByte", e);
		} finally {			
			closeJedis(client, broken);
		}	
		return ret;
	}

	@Override
	protected boolean delSortedSetBytesByBatch(byte[] key, byte[][] byteArray,int index) {
		boolean ret = false;
		Jedis client = null;
		boolean broken = false;
		if(key == null || byteArray == null){
			return ret;
		}
		try {
			client = this.redisPool.getResource();
			if(client != null){
				
				client.select(index);
				long flag = client.zrem(key, byteArray);
				if((flag <= byteArray.length) && (flag >= 0)){
					ret = true;
				}else{
					log.error(this.getName() +" delSortedSetBytesByBatch failed with key " + newString(key) + ", " + flag);
				}
			}
		} catch (Exception e) {
 			broken = handleException(e);
			log.error(this.getName() +" Error occurred when delSortedSetBytesByBatch", e);
		} finally {			
			closeJedis(client, broken);
		}	
		return ret;
	}

	@Override
	public boolean flushAll(String[] servers) {
		boolean ret = false;
		Jedis client = null;
		boolean broken = false;
		
		try {
			client = this.redisPool.getResource();
			if(client != null){
				client.flushAll();
				ret = true;
			}
		} catch (Exception e) {
 			broken = handleException(e);
		} finally {			
			closeJedis(client, broken);
		}	
		return ret;
	}
}
