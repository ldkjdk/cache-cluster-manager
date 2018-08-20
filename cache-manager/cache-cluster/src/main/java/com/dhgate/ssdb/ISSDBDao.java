package com.dhgate.ssdb;

import java.util.List;
import java.util.Map;

/**
 * The SSDB server access interface.
 * 
 * 
 * @author lidingkun
 *
 */
public interface ISSDBDao {

	/**
	 * Set key-value(String) pair with expire time to SSDB.
	 * 
	 * @param key
	 * @param value
	 * @param liveSeconds The key will expire after the seconds. -1 means never expire.
	 * @return True if success, otherwise false.
	 */
	public boolean setString(String key, String value, int liveSeconds);

	public boolean setBytes(String key, byte[] bytes, int liveSeconds);
	/**
	 * Set key-value(Object) pair with expire time to SSDB. 
	 * The object must implement Serializable interface.
	 * 
	 * @param key
	 * @param value
	 * @param liveSeconds The key will expire after the seconds. -1 means never expire.
	 * @return True if success, otherwise false.
	 */
	public boolean setObject(String key, Object value, int liveSeconds);

	/**
	 * Get String value by key from SSDB.
	 * 
	 * @param key
	 * @return The string object. If the key does not exist, return null.
	 */
	public String getString(String key);

	/**
	 * Get byte [] value by key from SSDB.
	 * 
	 * @param key
	 * @return The string object. If the key does not exist, return null.
	 */
	public byte[] getBytes(String key);

	/**
	 * Get Object value by key from SSDB.
	 * 
	 * @param key
	 * @return The object. If the key does not exist, return null.
	 */
	public Object getObject(String key);
	
	/**
	 * Delete the value by key from SSDB.
	 * There is a different usage with redis. This del method always return true no
	 * matter the key exists or not.
	 * 
	 * @param key
	 * @return True if success no matter the key exists or not.
	 */
	public boolean del(String key);

	/**
	 * Check whether the key exists in the SSDB.
	 * 
	 * @param key
	 * @return True if exists, otherwise false.
	 */
	public boolean isExist(String key);
	
	/**
	 * Increment the key by the specified value.
	 * 
	 * @param key
	 * @param incr
	 * @return The value after increment.
	 */
	public long incr(String key, long incr);
	
	/**
	 * Decrement the key by the specified value.
	 * 
	 * @param key
	 * @param decr
	 * @return The value after decrement.
	 */
	public long decr(String key, long decr);

	/**
	 * Make the key expired after live seconds.
	 * 
	 * @param key
	 * @param liveSeconds
	 * @return True if expires the key successfully, otherwise false.
	 */
	public boolean expire(String key, int liveSeconds);
	
	/**
	 * Get the expired time left for the key. The unit is second.
	 * 
	 * @param key
	 * @return The expired time left. Return -1 if the key is never expired.
	 */
	public long getExpireTime(String key);

	/**
	 * Set the key with score in the sorted set.
	 * 
	 * @param setName 
	 * @param key
	 * @param score
	 * @return True if success, otherwise false.
	 */
	public boolean zset(String setName, String key, long score);
	
	/**
	 * Delete the value by key in the sorted set.
	 * 
	 * @param setName
	 * @param key
	 * @return True if success, otherwise false.
	 */
	public boolean zdel(String setName, String key);
	
	/**
	 * Get the score by key from the sorted set.
	 * 
	 * @param setName
	 * @param key
	 * @return The score value.
	 */
	public Long zget(String setName, String key);
	
	/**
	 * Increment the key's score in the sorted set.
	 * 
	 * @param setName
	 * @param key
	 * @param incr
	 * @return The score after increment.
	 */
	public long zincr(String setName, String key, long incr);
	
	/**
	 * Decrement the key's score in the sorted set.
	 * 
	 * @param setName
	 * @param key
	 * @param decr
	 * @return The score after decrement.
	 */
	public long zdecr(String setName, String key, long decr);
	
	/**
	 * Get the count of sorted set from the start to end.
	 * The empty string means infinity. 
	 * 
	 * @param setName
	 * @return The count of key in the range.
	 */
	public long zcount(String setName, String start, String end);
	
	/**
	 * Clear the sorted set by set name.
	 * 
	 * @param setName
	 * @return True is success, otherwise false.
	 */
	public boolean zclear(String setName);
	
	/**
	 * Set the key-value(String) pair in hash map.
	 * 
	 * @param hashName
	 * @param key
	 * @param value
	 * @return True if success, otherwise false.
	 */
	public boolean hsetString(String hashName, String key, String value);
	
	/**
	 * Set the key-value(byte []) pair in hash map.
	 * 
	 * @param hashName
	 * @param key
	 * @param value
	 * @return True if success, otherwise false.
	 */
	
	public boolean hsetBytes(String hashName, String key, byte[] bytes);
	/**
	 * Set the key-value(Object) pair in hash map.
	 * 
	 * @param hashName
	 * @param key
	 * @param value
	 * @return True if success, otherwise false.
	 */
	public boolean hsetObject(String hashName, String key, Object value);
	
	/**
	 * Get the String value from hash map by key.
	 * 
	 * @param hashName
	 * @param key
	 * @return The String value. If the key does not exist, return null.
	 */
	public String hgetString(String hashName, String key);
	
	/**
	 * Get the byte [] value from hash map by key.
	 * 
	 * @param hashName
	 * @param key
	 * @return The String value. If the key does not exist, return null.
	 */
	public byte[] hgetBytes(String hashName, String key);
	/**
	 * Get the Object value from hash map by key.
	 * 
	 * @param hashName
	 * @param key
	 * @return The Object value. If the key does not exist, return null.
	 */
	public Object hgetObject(String hashName, String key);
	/**
	 * Get the Map<String,Object> from hash by hashName.
	 * 
	 * @param hashName
	 * @return The Map .
	 */
	public Map<String,Object> hgetall (String hashName);
	/**
	 * Get the Map<String,String> from hash by hashName.
	 * 
	 * @param hashName
	 * @return The Map .
	 */
	public Map<String,String> hgetallString (String hashName);
	/**
	 * Get the Map<String,byte[]> from hash by hashName.
	 * 
	 * @param hashName
	 * @return The Map .
	 */
	public Map<String,byte[]> hgetallByte (String hashName);
	
	
	/**
	 * Delete the value from hash map by key.
	 * 
	 * @param hashName
	 * @param key
	 * @return True if success, otherwise false.
	 */
	public boolean hdel(String hashName, String key);
	
	/**
	 * Get the size of hash map by map name.
	 * 
	 * @param hashName
	 * @return The size of hash map.
	 */
	public long hsize(String hashName);
	
	/**
	 * Clear the hash map.
	 * 
	 * @param hashName
	 * @return True if success, otherwise false.
	 */
	public boolean hclear(String hashName);
	
	
	public List<String> keys (String keyStart,String keyEnd,int limit);
	public boolean multiSet(Map<String,Object> map);
	public boolean multiSetString (Map<String,String> map);
	public boolean multiSetByte (Map<String,byte[]> map);
	
	public boolean multiHset(String hashName,Map<String,Object> map);
	public boolean multiHsetString(String hashName,Map<String,String> map);
	public boolean multiHsetByte (String hashName,Map<String,byte []> map);
	
	public Map<String,Object> multiHget (String hashName,String key []);
	public Map<String,byte[]> multiHgetByte (String hashName,String key []);
	public Map<String,String> multiHgetString (String hashName,String key []);
	
	public boolean multiHdel (String hashName,String key []);
	public Map<String,Object>  hScan (String hashName,String keyStart,String keyEnd,int limit);
	public Map<String,String>  hScanString (String hashName,String keyStart,String keyEnd,int limit);
	public Map<String,byte[]>  hScanByte (String hashName,String keyStart,String keyEnd,int limit);
	
	public List<Object> multiGet (String keys []);
	public List<byte[]> multiGetByte (String keys []);
	public List<String> multiGeString (String keys []);
	
	public boolean multiDel (String keys []);
	public List<String> hkeys (String hashName,String keyStart,String keyEnd,int limit);
	
	public String getHost(String key);
}
