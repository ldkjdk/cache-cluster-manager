package com.dhgate.redis;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.Set;
import java.util.UUID;

import org.slf4j.LoggerFactory;

import com.dhgate.memcache.IMemCached;
import com.dhgate.memcache.schooner.SchoonerSockIOPool;
import com.dhgate.redis.clients.jedis.JedisPoolConfig;

/**
 * The abstract class for JedisManager, including Sentinel, MultiWrite and Sharded.
 * Currently it support eleven kinds of operations: setString, delString, setObject, delObject,
 * expire, expireAt, lock, incr, isExist, getString, getObject. 
 * Each operation support four kinds of usage: only key/value pair, added db index, added poolkey 
 * and all together.  
 * 
 * @author lidingkun
 *
 */
public abstract class AbstractRedisDao implements IRedisDao,IMemCached {
	private org.slf4j.Logger log = LoggerFactory.getLogger(AbstractRedisDao.class);
	public static final String DEFAULT_ENCODING = "UTF-8";
	public static final String PING_RESPONSE = "PONG";
	public static final int DEFAULT_FAILOVER_SERVER_PORT = 6379;
	public static final int KEY_LEN_DEFAULT = 1024; //byte 
	public static final int VALUE_SIZE_DEFAULT = 1048576; //byte
	//public static final String DEFAULT_PING_FAIL_TRY_TIME = "10";
	//public static final String PING_RESPONSE = "PONG";
	
	//public static final String EMPTY_STRING = "";
	public static final String HOST_PORT_SEPERATOR = ":";
	public static final String SUCCESS_SET = "OK";
	
	protected volatile boolean  secondaryUsed = false;
	protected volatile boolean  secondaryPoling = false;
	
	public static final short STRING_TYPE = 0;
	public static final short OBJECT_TYPE = 1;
	public static final int REDIS_TIME_OUT = 2000;
	

	private boolean isSentinel=false;
	private String masterName;
    private boolean isSlave = false;
    private int hashingAlg = SchoonerSockIOPool.CONSISTENT_HASH;
	public boolean isSentinel() {
		return isSentinel;
	}
	
	
	public int getHashingAlg() {
		return hashingAlg;
	}



	public void setHashingAlg(int hashingAlg) {
		this.hashingAlg = hashingAlg;
	}



	@Override
	public String setString(String key, String str, int liveSeconds) {
		return setString( key, str, liveSeconds, 0);
	}
	@Override
	public boolean setStringByBatch(List<String> keyList, List<String> valueList, int liveSeconds){
		return setStringByBatch( keyList, valueList, liveSeconds, 0);
	}

	@Override
	public boolean setStringByBatch(List<String> keyList, List<String> valueList, int liveSeconds, int index){
		if (keyList == null || keyList.size() > 1000){
			log.error("The list is null or size is greater than 1000 in setStringByBatch, will not process.");
			return false;
		}
		if (keyList.size() != valueList.size()){
			log.error("The list is null or size is greater than 1000 in setStringByBatch, will not process.");
			return false;
		}
		
		List<byte[]> byteList = new ArrayList<byte[]>(valueList.size()); 
		for(String value : valueList){
			try {
				byteList.add(value.getBytes(DEFAULT_ENCODING));
			} catch (UnsupportedEncodingException e) {
				log.error("The value can not be encoded by default, " + value);
				return false;
			}
		}
		return setBytesByBatch(keyList, byteList, liveSeconds, index);
	}
	
	@Override
	public String getString(String key) {
		return getString(key, 0);
	}	
	
	@Override
	public List<String> getStringByBatch(List<String> keyList) {
		return getStringByBatch(keyList, 0);
	}
	
	@Override
	public List<String> getStringByBatch(List<String> keyList, int index) {
		if (keyList == null || keyList.size() > 1000){
			log.error("The list is null or size is greater than 1000 in getStringByBatch, will not process.");
			return null;
		}
		List<Object> ret = getBytesByBatch(keyList, index);
		List<String> returnList = null;
		if(ret != null){
			returnList = new ArrayList<String>(keyList.size());
			for(Object returnValue : ret){
				if(returnValue == null){
					returnList.add(null);
				}else{
					try {
						returnList.add(new String((byte[])returnValue, DEFAULT_ENCODING));
					} catch (UnsupportedEncodingException e) {
						log.error("Unsupported encoding exception when getStringByBatch", e);
					}
				}
			}
		}else{
			log.error("The return list is null in getStringByBatch.");
		}
		return returnList;
	}
	
	@Override
	public boolean isExist(String key){
		return isExist(key, 0);
	}	
	@Override
	public long delKey(String key) {
		return delKey (key, 0);
	}
	@Override
	public long delKey(String key, int index) {
		return del(key, index);
	}

	@Override
	public Object getObject(String key) {
		return getObject(key, 0);
	}
	
	@Override
	public Object getObject(String key, int index) {
		if (key == null)
			return null;
		byte[] ret = getBytes(key.getBytes(), index);
		if (ret == null)
			return null;
		return ObjectBytesExchange.toObject(ret);
	}
	@Override
	public List<Object> getObjectByBatch(List<String> keyList){
		return getObjectByBatch(keyList, 0);
	}
	@Override
	public List<Object> getObjectByBatch(List<String> keyList, int index){
		if (keyList == null || keyList.size() > 1000){
			log.error("The list is null or size is greater than 1000 in getObjectByBatch, return null.");
			return null;
		}
		List<Object> ret = getBytesByBatch(keyList, index);
		List<Object> returnList = null;
		if(ret != null){
			returnList = new ArrayList<Object>(keyList.size());
			for(Object returnValue : ret){
				if(returnValue == null){
					returnList.add(null);
				}else{
					returnList.add(ObjectBytesExchange.toObject((byte[])returnValue));
				}
			}
		}else{
			log.error("The return list is null in getObjectByBatch.");
		}
		return returnList;
	}
	@Override
	
	//Added for unify the usage with setString 
	public String setObject(String key, Object obj, int seconds){
		return setObject(key, obj, seconds, 0);
	}
	
	@Override
	public String setObject(String key, Object obj, int seconds, int index){
		if (key == null || obj == null)
			return null;
		byte[] byteObj = ObjectBytesExchange.toByteArray(obj);
		if (null == byteObj)
			return null;
		return setBytes(key.getBytes(), byteObj, seconds, index);
	}
	@Override
	public boolean setObjectByBatch(List<String> keyList, List<Object> valueList, int liveSeconds){
		return setObjectByBatch( keyList, valueList, liveSeconds);
	}
	@Override
	public boolean setObjectByBatch(List<String> keyList, List<Object> valueList, int liveSeconds, int index){
		if (keyList == null || keyList.size() > 1000){
			log.error("The list is null or size is greater than 1000 in setObjectByBatch, return null.");
			return false;
		}
		if (keyList.size() != valueList.size()){
			log.error("The key list size is not equal with value list size in setObjectByBatch, return null.");
			return false;
		}
		
		List<byte[]> byteList = new ArrayList<byte[]>(valueList.size()); 
		for(Object value : valueList){
			byteList.add(ObjectBytesExchange.toByteArray(value));
		}
		return setBytesByBatch(keyList, byteList, liveSeconds, index);
	}
	
	
	@Override
	public long incr(String key){
		return incr(key, 0);
	}
	@Override
	public long incrBy(String key, long integer){
		return this.incrBy(key, integer,0);
	}
	
	@Override	
	public long expire(String key, int seconds) {
		return expire(key, seconds, 0);
	}
	
	@Override
	public long expireAt(String key, long unixTime) {
		return expireAt(key,unixTime,0);
	}
	
	@Override
	public long getKeyExpiredTime(String key){
		return getKeyExpiredTime(key, 0);
	}
	@Override
	public long getKeyExpiredTime(String key, int index) {
		long ret = -1;
		try {
			ret = getKeyExpiredTime(key.getBytes(DEFAULT_ENCODING), index);
		} catch (UnsupportedEncodingException e) {
			log.error("Unsupported encoding exception when getKeyExpiredTime " + key, e);
			ret = -1;
		}
		
		return ret;
	}
	
	
	protected byte[] getBytes(String str){
		byte[] byteObj = null;
		try{
			byteObj = str.getBytes(DEFAULT_ENCODING);
		}catch(UnsupportedEncodingException e){
			log.error("Unsupported encoding exception when getBytes " + str, e);
			byteObj = null;
		}
		return byteObj;
	}
	
	protected String newString(byte[] byteObj){
		String str = null;
		try{
			str = new String(byteObj, DEFAULT_ENCODING);
		}catch(UnsupportedEncodingException e){
			log.error("Unsupported encoding exception when newString " + byteObj, e);
			str = null;
		}
		return str;
	}
	
	@Override
	public boolean setHashString(String key, String field, String value, int seconds){
		return setHashObjectByType(key, field, value, seconds, 0, STRING_TYPE);
	}
	
	@Override
	public boolean setHashString(String key, String field, String value,int seconds, int index) {
		return setHashObjectByType(key, field, value, seconds, index, STRING_TYPE);
	}
	
	
	public boolean setHashObject(String key, String field, Object value, int seconds){
		return setHashObjectByType(key, field, value, seconds, 0, OBJECT_TYPE);
	}
	
	private boolean setHashObjectByType(String key, String field, Object value, int seconds, int index, int type){
		boolean flag = false;
		if(key == null || field == null || value == null){
			log.error(" one of key, field, value is null in setHashObjectByType.");
		}else{
			byte[] byteObj = null;
			if(type == STRING_TYPE){
				byteObj = getBytes((String)value);
			}else {
				byteObj = ObjectBytesExchange.toByteArray(value);
			}
			flag = setHashBytes(getBytes(key), getBytes(field), byteObj, seconds, index);
		}
		return flag;
	}
	
	
	
	public String getHashString(String key, String field){
		return (String)getHashObjectByType(key, field, 0, STRING_TYPE);
	}
	
	public Object getHashObject(String key, String field){
		return getHashObjectByType(key, field, 0, OBJECT_TYPE);
	}
	
	private Object getHashObjectByType(String key, String field,int index,int type){
		Object obj = null;
		if(key == null || field == null){
			log.error("key or field is null in getHashObjectByType.");
		}else{
			byte[] byteObj = getHashBytes(getBytes(key), getBytes(field), index);
			if(byteObj != null){
				if(type == STRING_TYPE){
					obj = newString(byteObj);
				}else {
					obj = ObjectBytesExchange.toObject(byteObj);
				}

			}
		}
		return obj;
	}
	
	public long getHashLength(String key){
		return getHashLength(key, 0);
	}
		
	public boolean isHashFieldExist(String key, String field){
		return isHashFieldExist(key, field, 0);
	}
	@Override
	public List<String> getHashStringByBatch(String key, List<String> fields) {
		return this.getHashStringByBatch(key, fields,0);
	}
	
	public RedisLock getLock (String lockKey,long leaseTime,TimeUnit unit) {
		RedisLock lock = new RedisLock(false, UUID.randomUUID(), lockKey, this);
		lock.setLeaseTime(unit.toMillis(leaseTime));
		return lock;
	}
	
	@Override
	public RedisLock getLock (String lockKey) {
		return getLock(lockKey,1,TimeUnit.SECONDS);
	}

	
	@Override
	public List<String> getHashStringByBatch(String key, List<String> fields,int index){ 
		List<String> returnList = null;
		if(key == null || fields == null || fields.size() > 1000){
			log.error("key or fields is null or fields size greater than 1000 in getHashStringByBatch");
		}else{
			byte[][] byteArray = new byte[fields.size()][];
			for(int i = 0; i < byteArray.length; i++){
				byteArray[i] = getBytes(fields.get(i));
			}
			List<byte[]> result = getHashBytesByBatch(getBytes(key), byteArray, index);
			if(result != null && result.size() > 0){
				returnList = new ArrayList<String>(result.size());
				for(byte[] b : result){
					if(b != null){
						returnList.add(newString(b));
					}else{
						returnList.add(null);
					}
				}
			}
		}
		return returnList;
	}
	@Override
	public List<Object> getHashObjectByBatch(String key, List<String> fields){ 
		return getHashObjectByBatch(key, fields, 0);
	}
	@Override
	public List<Object> getHashObjectByBatch(String key, List<String> fields,int index){ 
		List<Object> returnList = null;
		if(key == null || fields == null || fields.size() > 1000){
			log.error("One of key, fields is null or fields size greater than 1000 in getHashObjectByBatch");
		}else{
			byte[][] byteArray = new byte[fields.size()][];
			for(int i = 0; i < byteArray.length; i++){
				byteArray[i] = getBytes(fields.get(i));
			}
			List<byte[]> result  = getHashBytesByBatch(getBytes(key), byteArray, index);
			if(result != null && result.size() > 0){
				returnList = new ArrayList<Object>(result.size());
				for(byte[] b : result){
					if(b != null){
						returnList.add(ObjectBytesExchange.toObject(b));
					}else{
						returnList.add(null);
					}
				}
			}
		}
		return returnList;
	}
	@Override
	public boolean setHashStringByBatch(String key, Map<String, String> hash, int seconds){
		return setHashStringByBatch(key, hash, seconds, 0);
	}
	@Override
	public boolean setHashStringByBatch(String key, Map<String, String> hash, int seconds,int index){
		boolean flag = false;
		if(key == null || hash == null || hash.size() > 1000){
			log.error("One of key, hash is null or fields size greater than 1000 in setHashStringByBatch");
		}else{
			Map<byte[], byte[]> map = new HashMap<byte[], byte[]>();
			for(Entry<String, String> entry : hash.entrySet()){
				map.put(getBytes(entry.getKey()), getBytes(entry.getValue()));
			}
			flag = setHashBytesByBatch(getBytes(key), map, seconds, index);
		}
		return flag;
	}
	
	@Override
	public boolean setHashObjectByBatch(String key, Map<String, Object> hash, int seconds){
		return setHashObjectByBatch(key, hash, seconds, 0);
	}
	@Override
	public boolean setHashObjectByBatch(String key, Map<String, Object> hash, int seconds,int index){
		boolean flag = false;
		if(key == null || hash == null || hash.size() > 1000){
			log.error("One of key, hash is null or fields size greater than 1000 in setHashObjectByBatch");
		}else{
			Map<byte[], byte[]> map = new HashMap<byte[], byte[]>();
			for(Entry<String, Object> entry : hash.entrySet()){
				map.put(getBytes(entry.getKey()), ObjectBytesExchange.toByteArray(entry.getValue()));
			}
			flag = setHashBytesByBatch(getBytes(key), map, seconds, index);
		}
		return flag;
	}
	
	@Override
	public boolean delHashField(String key, String field){
		return delHashField(key, field, 0);
	}
	@Override
	public boolean delHashField(String key, String field,int index){
		boolean flag = false;
		if(key == null || field == null){
			log.error("Key or field is null in delHashField.");
		}else{
			flag = delHashBytes(getBytes(key), getBytes(field), index);
		}
		return flag;
	}
	@Override
	public boolean delHashFieldByBatch(String key, List<String> fields){
		return delHashFieldByBatch(key, fields, 0);
	}
	@Override
	public boolean delHashFieldByBatch(String key, List<String> fields,int index){ 
		boolean flag = false;
		if(key == null || fields == null || fields.size() > 1000){
			log.error("Key or fields is null or fields size greater that 1000 in delHashFieldByBatch.");
		}else{
			byte[][] fieldArray = new byte[fields.size()][];
			for(int i = 0; i < fieldArray.length; i++){
				fieldArray[i] = getBytes(fields.get(i));
			}
			flag = delHashBytesByBatch(getBytes(key), fieldArray, index);
		}
		return flag;
	}
	@Override
	public boolean addSetString(String key, String member, int seconds){
		return addSetString(key, member, seconds, 0);
	}
	@Override
	public boolean addSetString(String key, String member, int seconds,int index){
		return addSetObjectWithType(key, member, seconds,STRING_TYPE,index);
	}
	@Override
	public boolean addSetObject(String key, Object member, int seconds){
		return addSetObject(key, member, seconds, 0);
	}
	@Override
	public boolean addSetObject(String key, Object member, int seconds,int index){
		return addSetObjectWithType(key, member, seconds,OBJECT_TYPE,index);
	}
	
	private boolean addSetObjectWithType(String key, Object member, int seconds, int type,int index){
		boolean flag = false;
		if(key == null || member == null){
			log.error("key or member is null in addSetObjectWithType");
		}else{
			byte[] byteObj = null;
			if(type == STRING_TYPE){
				byteObj = getBytes((String)member);
			}else{
				byteObj = ObjectBytesExchange.toByteArray(member);
			}
			flag = addSetBytes(getBytes(key), byteObj, seconds, index);
		}
		return flag;
	}
	
	
	@Override
	public boolean addSetStringByBatch(String key, List<String> member, int seconds){
		return addSetStringByBatch(key, member, seconds, 0);
	}
	
	@Override
	public boolean addSetStringByBatch(String key, List<String> member, int seconds,int index){
		boolean flag = false;
		if(key == null || member == null || member.size() > 1000){
			log.error("key or member is null or member size greater than 1000 in addSetStringByBatch");
		}else{
			byte[][] byteArray = new byte[member.size()][];
			for(int i = 0; i < byteArray.length; i++){
				byteArray[i] = getBytes(member.get(i));
			}
			flag = addSetBytesByBatch(getBytes(key), byteArray, seconds, index);
		}
		return flag;
	}
	
	@Override
	public boolean addSetObjectByBatch(String key, List<Object> member, int seconds){
		return addSetObjectByBatch(key, member, seconds, 0);
	}
	
	@Override
	public boolean addSetObjectByBatch(String key, List<Object> member, int seconds,int index){
		boolean flag = false;
		if(key == null || member == null || member.size() > 1000){
			log.error("key or member is null or member size greater than 1000 in addSetObjectByBatch");
		}else{
			byte[][] byteArray = new byte[member.size()][];
			for(int i = 0; i < byteArray.length; i++){
				byteArray[i] = ObjectBytesExchange.toByteArray(member.get(i));
			}
			flag = addSetBytesByBatch(getBytes(key), byteArray, seconds, index);
		}
		return flag;
	}

	@Override
	public long getSetSize(String key){
		return getSetSize(key, 0);
	}
	
	@Override
	public boolean isSetMemberExists(String key, String member){
		return isSetMemberExists(key, member, 0);
	}
	@Override
	public boolean isSetMemberExists(String key, String member,int index){
		return isSetMemberExistsWithType(key, member,STRING_TYPE,index);
	}
	@Override
	public boolean isSetMemberExists(String key, Object member){
		return isSetMemberExists(key, member, 0);
	}
	@Override
	public boolean isSetMemberExists(String key, Object member,int index){
		return isSetMemberExistsWithType(key, member,OBJECT_TYPE,index);
	}
	
	private boolean isSetMemberExistsWithType(String key, Object member, short type,int index){
		boolean flag = false;
		if( key == null || member == null){
			log.error("key or member is null in isSetMemberExistsWithType");
		}else{
			byte[] byteObj = null;
			if(type == STRING_TYPE){
				byteObj = getBytes((String)member);
			}else{
				byteObj = ObjectBytesExchange.toByteArray(member);
			}
			flag = isSetByteExists(getBytes(key), byteObj,index);
		}
		return flag;
	}
	@Override
	public boolean delSetString(String key, String member){
		return delSetString(key, member, 0);
	}
	@Override
	public boolean delSetString(String key, String member,int index){
		return delSetObjectWithType(key, member,STRING_TYPE,index);
	}
	@Override
	public boolean delSetObject(String key, Object member){
		return delSetObject(key, member, 0);
	}
	@Override
	public boolean delSetObject(String key, Object member,int index){
		return delSetObjectWithType(key, member,OBJECT_TYPE,index);
	}
	
	private boolean delSetObjectWithType(String key, Object member, short type,int index){
		boolean flag = false;
		if(key == null || member == null){
			log.error("key or member is null in delSetObjectWithType");
		}else{
			byte[] byteObj = null;
			if(type == STRING_TYPE){
				byteObj = getBytes((String)member);
			}else{
				byteObj = ObjectBytesExchange.toByteArray(member);
			}
			flag = delSetByte(getBytes(key), byteObj,index);
		}
		return flag;
	}
	@Override
	public boolean delSetStringByBatch(String key, List<String> member){
		return delSetStringByBatch(key, member, 0);
	}
	@Override
	public boolean delSetStringByBatch(String key, List<String> member,int index){
		boolean flag = false;
		if(key == null || member == null || member.size() > 1000){
			log.error("key or member is null or member size greater than 1000 in delSetStringByBatch");
		}else{
			byte[][] byteArray = new byte[member.size()][];
			for(int i = 0; i < byteArray.length; i++){
				byteArray[i] = getBytes(member.get(i));
			}
			flag = delSetBytesByBatch(getBytes(key), byteArray,index);
		}
		return flag;
	}
	@Override
	public boolean delSetObjectByBatch(String key, List<Object> member){
		return delSetObjectByBatch(key, member, 0);
	}
	@Override
	public boolean delSetObjectByBatch(String key, List<Object> member,int index){
		boolean flag = false;
		if(key == null || member == null || member.size() > 1000){
			log.error("key or member is null or member size greater than 1000 in delSetObjectByBatch");
		}else{
			byte[][] byteArray = new byte[member.size()][];
			for(int i = 0; i < byteArray.length; i++){
				byteArray[i] = ObjectBytesExchange.toByteArray(member.get(i));
			}
			flag = delSetBytesByBatch(getBytes(key), byteArray,index);
		}
		return flag;
	}
	@Override
	public Set<String> getAllSetString(String key){
		return getAllSetString(key, 0);
	}
	@Override
	public Set<String> getAllSetString(String key,int index){
		Set<String> returnList = null;
		if(key == null){
			log.error("key is null in getAllSetString");
		}else{
			Set<byte[]> byteList = getAllSetBytes(getBytes(key),index);
			if(byteList != null){
				returnList = new LinkedHashSet<String>(byteList.size());
				for(byte[] b : byteList){
					if(b == null){
						returnList.add(null);
					}else{
						returnList.add(newString(b));
					}
				}
			}
		}
		return returnList;
	}
	@Override
	public Set<Object> getAllSetObject(String key){
		return getAllSetObject(key, 0);
	}
	@Override
	public Set<Object> getAllSetObject(String key,int index){
		Set<Object> returnList = null;
		if(key == null){
			log.error("key is null in getAllSetObject");
		}else{
			Set<byte[]> byteList = getAllSetBytes(getBytes(key),index);
			if(byteList != null){
				returnList = new LinkedHashSet<Object>(byteList.size());
				for(byte[] b : byteList){
					if(b == null){
						returnList.add(null);
					}else{
						returnList.add(ObjectBytesExchange.toObject(b));
					}
				}
			}
		}
		return returnList;
	}
	@Override
	public boolean addSortedSetString(String key, String member, double score, int seconds){
		return addSortedSetString(key, member, score, seconds, 0);
	}
	@Override
	public boolean addSortedSetString(String key, String member, double score, int seconds,int index){
		return addSortedSetObjectByType(key, member, score, seconds,STRING_TYPE,index);
	}
	@Override	
	public boolean addSortedSetObject(String key, Object member, double score, int seconds){
		return addSortedSetObject(key, member, score, seconds, 0);
	}
	@Override
	public boolean addSortedSetObject(String key, Object member, double score, int seconds,int index){
		return addSortedSetObjectByType(key, member, score, seconds,OBJECT_TYPE,index);
	}
	
	private boolean addSortedSetObjectByType(String key, Object member, double score, int seconds, short type,int index){
		boolean flag = false;
		if(key == null || member == null ){
			log.error("key or member is null in addSortedSetObjectByType");
		}else{
			byte[] byteObj = null;
			if(type == STRING_TYPE){
				byteObj = getBytes((String)member);
			}else{
				byteObj = ObjectBytesExchange.toByteArray(member);
			}
			flag = addSortedSetByte(getBytes(key), byteObj, score, seconds,index);
		}
		return flag;
	}
	@Override
	public boolean addSortedSetStringByBatch(String key, Map<String, Double> members, int seconds){
		return addSortedSetStringByBatch(key, members, seconds, 0);
	}
	@Override
	public boolean addSortedSetStringByBatch(String key, Map<String, Double> members, int seconds,int index){
		boolean flag = false;
		if(key == null || members == null || members.size() > 1000){
			log.error("key or members is null or members size greater than 1000 in addSortedSetStringByBatch");
		}else{
			Map<byte[], Double> memberBytes = new HashMap<byte[], Double>();
			for(Entry<String, Double> entry : members.entrySet()){
				memberBytes.put(getBytes(entry.getKey()), entry.getValue());
			}
			flag = addSortedSetByteByBatch(getBytes(key), memberBytes, seconds,index);
		}
		return flag;
	}
	@Override
	public boolean addSortedSetObjectByBatch(String key, Map<Object, Double> members, int seconds){
		return addSortedSetObjectByBatch(key, members, seconds, 0);
	}
	@Override
	public boolean addSortedSetObjectByBatch(String key, Map<Object, Double> members, int seconds,int index){
		boolean flag = false;
		if(key == null || members == null || members.size() > 1000){
			log.error("key or members is null or members size greater than 1000 in addSortedSetObjectByBatch");
		}else{
			Map<byte[], Double> memberBytes = new HashMap<byte[], Double>();
			for(Entry<Object, Double> entry : members.entrySet()){
				memberBytes.put(ObjectBytesExchange.toByteArray(entry.getKey()), entry.getValue());
			}
			flag = addSortedSetByteByBatch(getBytes(key), memberBytes, seconds,index);
		}
		return flag;
	}
	
	@Override
	public long getSortedSetSize(String key){
		return getSortedSetSize(key, 0);
	}
	
	@Override
	public double incrSortedSetStringScore(String key, String member, double score){
		return incrSortedSetStringScore(key, member, score, 0);
	}
	@Override
	public double incrSortedSetStringScore(String key, String member, double score,int index){
		return incrSortedSetByteScoreWithType(key, member, score,STRING_TYPE,index);
	}
	@Override
	public double incrSortedSetObjectScore(String key, Object member, double score){
		return incrSortedSetObjectScore(key, member, score, 0);
	}
	@Override
	public double incrSortedSetObjectScore(String key, Object member, double score,int index){
		return incrSortedSetByteScoreWithType(key, member, score,OBJECT_TYPE,index);
	}
	
	private double incrSortedSetByteScoreWithType(String key, Object member, double score, short type,int index){
		double ret = 0;
		if(key == null || member == null){
			log.error("key or member is null in incrSortedSetByteScoreWithType");
		}else{
			byte[] byteObj = null;
			if(type == STRING_TYPE){
				byteObj = getBytes((String)member);
			}else{
				byteObj = ObjectBytesExchange.toByteArray(member);
			}
			ret = incrSortedSetByteScore(getBytes(key), byteObj, score,index);
		}
		return ret;
	}
	@Override
	public Set<String> getSortedSetStringRange(String key, long start, long end){
		return getSortedSetStringRange(key, start, end, 0);
	}
	@Override
	public Set<String> getSortedSetStringRange(String key, long start, long end,int index){
		Set<String> sc = null;
		if(key == null){
			log.error("key or member is null in getSortedSetStringRange");
		}else{
			Set<byte[]> set = getSortedSetByteRange(getBytes(key), start, end,index);
			if(set != null){
				sc = new LinkedHashSet<String>(set.size());
				for(byte[] byteObj : set){
					if(byteObj == null){
						sc.add(null);
					}else{
						sc.add(newString(byteObj));
					}
				}
			}
			
		}
		return sc;
	}
	@Override
	public Set<Object> getSortedSetObjectRange(String key, long start, long end){
		return getSortedSetObjectRange(key, start, end, 0);
	}
	@Override
	public Set<Object> getSortedSetObjectRange(String key, long start, long end,int index){
		Set<Object> sc = null;
		if(key == null){
			log.error("key or member is null in getSortedSetObjectRange");
		}else{
			Set<byte[]> set = getSortedSetByteRange(getBytes(key), start, end,index);
			if(set != null){
				sc = new LinkedHashSet<Object>(set.size());
				for(byte[] byteObj : set){
					if(byteObj == null){
						sc.add(null);
					}else{
						sc.add(ObjectBytesExchange.toObject(byteObj));
					}
				}
			}			
		}
		return sc;
	}
	@Override
	public Set<String> getSortedSetStringRangeOrderByScore(String key, long start, long end){
		return getSortedSetStringRangeOrderByScore(key, start, end, 0);
	}
	@Override
	
	public Set<String> getSortedSetStringRangeOrderByScore(String key, long start, long end,int index){
		Set<String> sc = null;
		if(key == null){
			log.error("key or member is null in getSortedSetStringRevRange");
		}else{
			Set<byte[]> set = getSortedSetByteRevRange(getBytes(key), start, end,index);
			if(set != null){
				sc = new LinkedHashSet<String>(set.size());
				for(byte[] byteObj : set){
					if(byteObj == null){
						sc.add(null);
					}else{
						sc.add(newString(byteObj));
					}
				}
			}
			
		}
		return sc;
	}
	@Override
	public Set<Object> getSortedSetObjectRangeOrderByScore(String key, long start, long end){
		return getSortedSetObjectRangeOrderByScore(key, start, end, 0);
	}
	@Override
	public Set<Object> getSortedSetObjectRangeOrderByScore(String key, long start, long end,int index){
		Set<Object> sc = null;
		if(key == null){
			log.error("key or member is null in getSortedSetObjectRevRange");
		}else{
			Set<byte[]> set = getSortedSetByteRevRange(getBytes(key), start, end,index);
			if(set != null){
				sc = new LinkedHashSet<Object>(set.size());
				for(byte[] byteObj : set){
					if(byteObj == null){
						sc.add(null);
					}else{
						sc.add(ObjectBytesExchange.toObject(byteObj));
					}
				}
			}			
		}
		return sc;
	}
	@Override
	public boolean delSortedSetString(String key, String member){
		return delSortedSetString(key, member, 0);
	}
	@Override
	public boolean delSortedSetString(String key, String member,int index){
		return delSortedSetObjectWithType(key, member,STRING_TYPE,index);
	}
	@Override
	public boolean delSortedSetObject(String key, Object member){
		return delSortedSetObject(key, member, 0);
	}
	@Override
	public boolean delSortedSetObject(String key, Object member,int index){
		return delSortedSetObjectWithType(key, member, OBJECT_TYPE,index);
	}
	
	private boolean delSortedSetObjectWithType(String key, Object member, short type,int index){
		boolean flag = false;
		if(key == null || member == null){
			log.error("key or member is null in delSortedSetObjectWithType");
		}else{
			byte[] byteObj = null;
			if(type == STRING_TYPE){
				byteObj = getBytes((String)member);
			}else{
				byteObj = ObjectBytesExchange.toByteArray(member);
			}
			flag = delSortedSetByte(getBytes(key), byteObj,index);
		}
		return flag;
	}
	@Override
	public boolean delSortedSetStringByBatch(String key, List<String> member){
		return delSortedSetStringByBatch(key, member, 0);
	}
	@Override
	public boolean delSortedSetStringByBatch(String key, List<String> member,int index){
		boolean flag = false;
		if(key == null || member == null || member.size() > 1000){
			log.error("key or member is null or member size greater than 1000 in delSortedSetStringByBatch");
		}else{
			byte[][] byteArray = new byte[member.size()][];
			for(int i = 0; i < byteArray.length; i++){
				byteArray[i] = getBytes(member.get(i));
			}
			flag = delSortedSetBytesByBatch(getBytes(key), byteArray,index);
		}
		return flag;
	}
	@Override
	public boolean delSortedSetObjectByBatch(String key, List<Object> member){
		return delSortedSetObjectByBatch(key, member, 0);
	}
	@Override
	public boolean delSortedSetObjectByBatch(String key, List<Object> member,int index){
		boolean flag = false;
		if(key == null || member == null || member.size() > 1000){
			log.error("key or member is null or member size greater than 1000 in delSetObjectByBatch");
		}else{
			byte[][] byteArray = new byte[member.size()][];
			for(int i = 0; i < byteArray.length; i++){
				byteArray[i] = ObjectBytesExchange.toByteArray(member.get(i));
			}
			flag = delSortedSetBytesByBatch(getBytes(key), byteArray,index);
		}
		return flag;
	}
	
	
	@Override
	public long decr(String key) {
		return this.decr(key,0);
	}
	@Override
	public long decrBy(String key,long integer){
		return this.decrBy(key,integer,0);
	}
	
	@Override
	public boolean delKeyByBatch(List<String> keyList){
		return delKeyByBatch(keyList, 0);
	}
	
	@Override
	public Map<String, String> hgetAll(String key) {
		return hgetAll(key, 0);
	}

	@Override
	public Map<String, Object> getHashAllObject(String key) {
		return getHashAllObject(key, 0);
	}
	
	@Override
	public boolean set(String key, Object value) {
		return set(key, value, 0);
	}

	@Override
	public boolean set(String key, Object value, Date expiry) {
		long exp=(expiry.getTime()>System.currentTimeMillis())?(expiry.getTime()-System.currentTimeMillis()):expiry.getTime();
		
		if (key == null || value == null)
			return false;
		byte[] byteObj = ObjectBytesExchange.toByteArray(value);
		if (null == byteObj)
			return false;
		return setBytes(key.getBytes(), byteObj,(int) exp/1000, 0,0);
	}

	@Override
	public boolean set(String key, Object value, long liveTime) {
		int exp=(int)liveTime;
		if (key == null || value == null)
			return false;
		byte[] byteObj = ObjectBytesExchange.toByteArray(value);
		if (null == byteObj)
			return false;
		return setBytes(key.getBytes(), byteObj, exp, 0,0);
	}

	@Override
	public boolean add(String key, Object value) {
		return this.add(key, value, 0);
	}

	@Override
	public boolean add(String key, Object value, Date expiry) {
		long time=(expiry.getTime()>System.currentTimeMillis())?(expiry.getTime()-System.currentTimeMillis()):expiry.getTime();
		
		if (key == null || value == null)
			return false;
		byte[] byteObj = ObjectBytesExchange.toByteArray(value);
		if (null == byteObj)
			return false;
		return setBytes(key.getBytes(), byteObj,(int) time/1000, 0,1);
		
	}

	@Override
	public boolean add(String key, Object value, long liveTime) {
		int exp=(int)liveTime;
		if (key == null || value == null)
			return false;
		byte[] byteObj = ObjectBytesExchange.toByteArray(value);
		if (null == byteObj)
			return false;
		return setBytes(key.getBytes(), byteObj, exp, 0,1);
	}

	@Override
	public boolean replace(String key, Object value) {
		return this.replace (key, value, 0);
	}

	@Override
	public boolean replace(String key, Object value, Date expiry) {
		long time=(expiry.getTime()>System.currentTimeMillis())?(expiry.getTime()-System.currentTimeMillis()):expiry.getTime();
		if (key == null || value == null)
			return false;
		byte[] byteObj = ObjectBytesExchange.toByteArray(value);
		if (null == byteObj)
			return false;
		return setBytes(key.getBytes(), byteObj,(int) time/1000, 0,2);
	}

	@Override
	public boolean replace(String key, Object value, long liveTime) {
		int exp=(int)liveTime;
		if (key == null || value == null)
			return false;
		byte[] byteObj = ObjectBytesExchange.toByteArray(value);
		if (null == byteObj)
			return false;
		return setBytes(key.getBytes(), byteObj, exp, 0,2);
	}
	
	
	public String getMasterName() {
		return masterName;
	}

	public void setMasterName(String masterName) {
		this.masterName = masterName;
	}

	public void setSentinel(boolean isSentinel) {
		this.isSentinel = isSentinel;
	}

	
	public boolean isSlave() {
		return isSlave;
	}

	public void setSlave(boolean isSlave) {
		this.isSlave = isSlave;
	}

	 protected  Map<String, Object> convert (Map<byte[],byte []> ret) {
	        Map<String,Object> rs = null;
	      try {
	            if (ret != null) {
	                rs = new HashMap<String, Object> ();
	                for (byte [] keys:ret.keySet()) {
	                    String k = new String(keys);
	                    byte [] v = ret.get(keys);
	                    Object o = null;
	                    if (v != null) {
	                        o = ObjectBytesExchange.toObject(v);
	                    }
	                    rs.put(k, o);
	                }
	            }
	      } catch (Exception e) {
	          log.error("",e);
	      }
	         
	        return rs;
	    }
	
	 	@Override
		public boolean flushAll() {
			return this.flushAll(null);
		}

	
	protected abstract byte[] getBytes(byte[] key, int index);
	
	protected abstract String setBytes(byte[] key, byte[] bytes, int seconds, int index);
	
	protected abstract long del(String key, int index);

	protected abstract boolean setBytesByBatch(List<String> keyList, List<byte[]> valueList, int liveSeconds, int index);
	
	protected abstract List<Object> getBytesByBatch(List<String> keyList, int index);
	
	protected abstract long getKeyExpiredTime(byte[] key, int index);

	//added in 1.3.6
	protected abstract boolean setHashBytes(byte[] key, byte[] field, byte[] value, int seconds,int index);
	protected abstract byte[] getHashBytes(byte[]key, byte[] field,int index);
	protected abstract List<byte[]> getHashBytesByBatch(byte[] key, byte[][] fieldArray,int index);
	
	protected abstract boolean setHashBytesByBatch(byte[] key, Map<byte[], byte[]> map, int seconds,int index);
	
	protected abstract boolean delHashBytes(byte[] key, byte[] field,int index);
	
	protected abstract boolean delHashBytesByBatch(byte[] key, byte[][] fieldArray,int index);
	
	protected abstract boolean addSetBytes(byte[] key, byte[] member, int seconds,int index);
	 
	protected abstract boolean addSetBytesByBatch(byte[] key, byte[][] byteArray, int seconds,int index);
	 
	protected abstract boolean isSetByteExists(byte[] key, byte[] member,int index);
	 
	protected abstract boolean delSetByte(byte[] key, byte[] member,int index);
	 
	protected abstract boolean delSetBytesByBatch(byte[] key, byte[][] byteArray,int index);
	 
	protected abstract Set<byte[]> getAllSetBytes(byte[] key,int index);
	
	protected abstract boolean addSortedSetByte(byte[] key, byte[] member, double score, int seconds,int index);
	
	protected abstract boolean addSortedSetByteByBatch(byte[] key, Map<byte[], Double> memberBytes, int seconds,int index);
	
	public abstract long getSortedSetSize(String key,int index);
	
	protected abstract double incrSortedSetByteScore(byte[] key, byte[] member, double score,int index);
	
	protected abstract Set<byte[]> getSortedSetByteRange(byte[] key, long start, long end,int index);
	
	protected abstract Set<byte[]> getSortedSetByteRevRange(byte[] key, long start, long end,int index);
	
	protected abstract boolean delSortedSetByte(byte[] key, byte[] member,int index);
	 
	protected abstract boolean delSortedSetBytesByBatch(byte[] key, byte[][] byteArray,int index);
	protected abstract boolean setBytes(byte[] key, byte[] bytes, int seconds, int index,int flag);
	public void updatePool (String servers[],int hashingAlg,int weights[],JedisPoolConfig conf,int timeout) {
		throw new RuntimeException("updatePool not support ,need to Override") ;
	}
}
