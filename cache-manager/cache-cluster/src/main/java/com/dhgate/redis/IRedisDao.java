package com.dhgate.redis;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.dhgate.redis.clients.jedis.JedisPubSub;

/**
 * The redis server access interface.
 * 
 * 
 * @author lidingkun
 *
 */
public interface IRedisDao {

	/**
	 * setString
	 * @param key
	 * @param str
	 * @param liveSeconds
	 * @return OK if success, otherwise null.
	 */
	public String setString(String key, String str, int liveSeconds);
	
	/**
	 * setString
	 * @param key
	 * @param str
	 * @param liveSeconds
	 * @param index
	 * @return OK if success, otherwise null.
	 */
	public String setString(String key, String str, int liveSeconds, int index);
	
	
	/**
	 * setStringByBatch
	 * @param keyList
	 * @param valueList
	 * @param liveSeconds
	 * @return True if all key value pairs have been set successfully, otherwise false.
	 */
	public boolean setStringByBatch(List<String> keyList, List<String> valueList, int liveSeconds);
	
	
	/**
	 * setStringByBatch
	 * @param keyList
	 * @param valueList
	 * @param liveSeconds
	 * @param index
	 * @return True if all key value pairs have been set successfully, otherwise false.
	 */
	public boolean setStringByBatch(List<String> keyList, List<String> valueList, int liveSeconds,int index);
	
	/**
	 * getString
	 * @param key
	 * @return The value of key, or null if key does not exist.
	 */
	public String getString(String key);
	
	/**
	 * getString
	 * @param key
	 * @param index
	 * @return The value of key, or null if key does not exist.
	 */
	public String getString(String key, int index);
	
	
	/**
	 * getStringByBatch
	 * @param keyList
	 * @return The value list of key list. If one key has not specified value, null would be returned in the value list.
	 */
	public List<String> getStringByBatch(List<String> keyList);
	
	public List<String> getStringByBatch(List<String> keyList,int index);
	
	
	/**
	 * delString
	 * @param key
	 * @return 1 if success, otherwise 0. If exception thrown, the value is -1.
	 */
	public long delKey (String key);
	
	/**
	 * delString
	 * @param key
	 * @param index
	 * @return 1 if success, otherwise 0. If exception thrown, the value is -1.
	 */
	public long delKey (String key, int index);
	
	
	
	/**
	 * delKeyBatch
	 * @param keyList
	 * @return True if all keys in the list has been deleted, otherwise false.
	 */
	public boolean delKeyByBatch(List<String> keyList);
	
	/**
	 * delKeyByBatch
	 * @param keyList
	 * @param index
	 * @return True if all keys in the list has been deleted, otherwise false.
	 */
	public boolean delKeyByBatch(List<String> keyList, int index);
	
	/**
	 * isExist
	 * @param key
	 * @return True if key exists, otherwise false.
	 */
	public boolean isExist(String key);
	
	/**
	 * isExist
	 * @param key
	 * @param index
	 * @return True if key exists, otherwise false.
	 */
	public boolean isExist(String key, int index);
	

	/**
	 * getObject
	 * @param key
	 * @return The value of key, or null if key does not exist.
	 */
	public Object getObject(String key);
	
	/**
	 * getObject
	 * @param key
	 * @param index
	 * @return The value of key, or null if key does not exist.
	 */
	public Object getObject(String key, int index);
	
	/**
	 * getObjectByBatch
	 * @param keyList
	 * @return The value list of key list. If one key has not specified value, null would be returned in the value list.
	 */
	public List<Object> getObjectByBatch(List<String> keyList);
	
	public List<Object> getObjectByBatch(List<String> keyList, int index);
	

	/**
	 * setObject
	 * @param key
	 * @param obj
	 * @param seconds
	 * @return OK if success, otherwise null.
	 */
	public String setObject(String key, Object obj, int seconds);
	
	/**
	 * setObject
	 * @param key
	 * @param obj
	 * @param seconds
	 * @param index
	 * @return OK if success, otherwise null.
	 */
	public String setObject(String key, Object obj, int seconds, int index);
	
	/**
	 * setObjectByBatch
	 * @param keyList
	 * @param valueList
	 * @param liveSeconds
	 * @return True if all key value pairs have been set successfully, otherwise false.
	 */
	public boolean setObjectByBatch(List<String> keyList, List<Object> valueList, int liveSeconds);
	
	public boolean setObjectByBatch(List<String> keyList, List<Object> valueList, int liveSeconds,int index);
	/**
	 * getLock
	 * @param key
	 * @param leaseTime 
	 * @return RedisLock
	 */
	public RedisLock getLock (String lockKey,long leaseTime,TimeUnit unit);
	
	/**
	 * getLock with leeseTime default 1000ms
	 * @see getLock (String lockKey,long leaseTime)
	 * @param lockKey
	 * @return RedisLock
	 */
	public RedisLock getLock (String lockKey);
	public Object eval(String script, List<String> keys, List<String> args) throws Exception;
	/**
	 * incr
	 * @param key
	 * @return The value after increment, otherwise 0. If exception thrown, the value is -1.
	 */
	public long incr(String key);
	
	/**
	 * incr
	 * @param key
	 * @param index
	 * @return The value after increment, otherwise 0. If exception thrown, the value is -1.
	 */
	public long incr(String key, int index);

	/**
	 * expire
	 * @param key
	 * @param seconds
	 * @return 1 if the timeout is set, otherwise 0. If exception thrown, the value is -1.
	 */
	public long expire(String key, int seconds);
	
	/**
	 * expire
	 * @param key
	 * @param seconds
	 * @param index
	 * @return 1 if the timeout is set, otherwise 0. If exception thrown, the value is -1.
	 */
	public long expire(String key, int seconds, int index);

	/**
	 * expireAt
	 * @param key
	 * @param unixTime
	 * @return 1 if the timeout is set, otherwise 0. If exception thrown, the value is -1.
	 */
	public long expireAt(String key, long unixTime);
	
	/**
	 * expireAt
	 * @param key
	 * @param unixTime
	 * @param index
	 * @return 1 if the timeout is set, otherwise 0. If exception thrown, the value is -1.
	 */
	public long expireAt(String key, long unixTime, int index);

	
	/**
	 * getKeyExpiredTime
	 * @param key
	 * @return The key expired time. -1 means no time limit or not existed in 2.6.16.
	 */
	public long getKeyExpiredTime(String key);
	
	
	/**
	 * getKeyExpiredTime
	 * @param key
	 * @param index
	 * @return The key expired time. -1 means no time limit or not existed in 2.6.16.
	 */
	public long getKeyExpiredTime(String key, int index);
	
	/* hash map */
	/**
	 * setHashString
	 * @param key
	 * @param field
	 * @param value
	 * @param seconds
	 * @return True if the field/value is set successfully in the key's hash map and the key is expired by specified seconds. 
	 */
	public boolean setHashString(String key, String field, String value, int seconds);
	public boolean setHashString(String key, String field, String value, int seconds,int index);
	
	/**
	 * setHashObject
	 * @param key
	 * @param field
	 * @param value
	 * @param seconds
	 * @return True if the field/value is set successfully in the key's hash map and the key is expired by specified seconds. 
	 */
	public boolean setHashObject(String key, String field, Object value, int seconds);
	
	
	/**
	 * getHashString
	 * @param key
	 * @param field
	 * @return The value for the field in the key's hash map.
	 */
	public String getHashString(String key, String field);
	
	
	
	/**
	 * getHashObject
	 * @param key
	 * @param field
	 * @return The value for the field in the key's hash map.
	 */
	public Object getHashObject(String key, String field);
	
	
	/**
	 * getHashLength
	 * @param key
	 * @return The size of the key's hash map.
	 */
	public long getHashLength(String key);
	public long getHashLength(String key,int index);
	
	/**
	 * isHashFieldExist
	 * @param key
	 * @param field
	 * @return True if the field exists in the key's hash map, otherwise false.
	 */
	public boolean isHashFieldExist(String key, String field);
	public boolean isHashFieldExist(String key, String field,int index);
	/**
	 * getHashStringByBatch
	 * @param key
	 * @param fields
	 * @return The value list for the field list in the key's hash map. If one field has not specified value, 
	 * null would be returned in the value list.
	 */
	public List<String> getHashStringByBatch(String key, List<String> fields); 
	public List<String> getHashStringByBatch(String key, List<String> fields,int index);
	
	/**
	 * getHashObjectByBatch
	 * @param key
	 * @param fields
	 * @return The value list for the field list in the key's hash map. If one field has not specified value, 
	 * null would be returned in the value list.
	 */
	public List<Object> getHashObjectByBatch(String key, List<String> fields); 
	public List<Object> getHashObjectByBatch(String key, List<String> fields,int index); 
	
	
	/**
	 * setHashStringByBatch
	 * @param key
	 * @param hash
	 * @param seconds
	 * @return True if field/value pairs have been set successfully in the key's hash map and the 
	 * key is expired by specified seconds.
	 */
	public boolean setHashStringByBatch(String key, Map<String, String> hash, int seconds);
	
	public boolean setHashStringByBatch(String key, Map<String, String> hash, int seconds,int index);
	/**
	 * setHashObjectByBatch
	 * @param key
	 * @param hash
	 * @param seconds
	 * @return True if field/value pairs have been set successfully in the key's hash map and the 
	 * key is expired by specified seconds.
	 */
	public boolean setHashObjectByBatch(String key, Map<String, Object> hash, int seconds);
	
	public boolean setHashObjectByBatch(String key, Map<String, Object> hash, int seconds,int index);
	/**
	 * delHashField
	 * @param key
	 * @param field
	 * @return True if the field is removed from the key's hash map.
	 */
	public boolean delHashField(String key, String field);
	
	public boolean delHashField(String key, String field,int index);
	
	/**
	 * delHashFieldByBatch
	 * @param key
	 * @param fields
	 * @return True if the fields in the list are removed from the key's hash map.
	 */
	public boolean delHashFieldByBatch(String key, List<String> fields); 
	public boolean delHashFieldByBatch(String key, List<String> fields,int index);
	
	
	/* set */
	/**
	 * addSetString
	 * @param key
	 * @param member
	 * @param seconds
	 * @return True if the member is set to the key's set successfully and the key is expired by specified seconds.
	 */
	public boolean addSetString(String key, String member, int seconds);
	public boolean addSetString(String key, String member, int seconds,int index);
	
	/**
	 * addSetStringByBatch
	 * @param key
	 * @param member
	 * @param seconds
	 * @return True if the member list is set to the key's set successfully and the key is expired by specified seconds.
	 */
	public boolean addSetStringByBatch(String key, List<String> member, int seconds);
	public boolean addSetStringByBatch(String key, List<String> member, int seconds,int index);
	
	
	/**
	 * addSetObject
	 * @param key
	 * @param member
	 * @param seconds
	 * @return True if the member is set to the key's set successfully and the key is expired by specified seconds.
	 */
	public boolean addSetObject(String key, Object member, int seconds);
	public boolean addSetObject(String key, Object member, int seconds,int index);
	/**
	 * addSetObjectByBatch
	 * @param key
	 * @param member
	 * @param seconds
	 * @return True if the member list is set to the key's set successfully and the key is expired by specified seconds.
	 */
	public boolean addSetObjectByBatch(String key, List<Object> member, int seconds);
	public boolean addSetObjectByBatch(String key, List<Object> member, int seconds,int index);
	
	/**
	 * getSetSize
	 * @param key
	 * @return The size of the key's set.
	 */
	public long getSetSize(String key);
	public long getSetSize(String key,int index);
	
	
	/**
	 * isSetMemberExists
	 * @param key
	 * @param member
	 * @return True if the member exists in the key's set, otherwise false.
	 */
	public boolean isSetMemberExists(String key, String member);
	public boolean isSetMemberExists(String key, String member,int index);
	
	/**
	 * isSetMemberExists
	 * @param key
	 * @param member
	 * @return True if the member exists in the key's set, otherwise false.
	 */
	public boolean isSetMemberExists(String key, Object member);
	public boolean isSetMemberExists(String key, Object member,int index);
	
	/**
	 * delSetString
	 * @param key
	 * @param member
	 * @return True is the member is removed from the key's set.
	 */
	public boolean delSetString(String key, String member);
	public boolean delSetString(String key, String member,int index);
	/**
	 * delSetObject
	 * @param key
	 * @param member
	 * @return True is the member is removed from the key's set.
	 */
	public boolean delSetObject(String key, Object member);
	public boolean delSetObject(String key, Object member,int index);
	
	/**
	 * delSetStringByBatch
	 * @param key
	 * @param member
	 * @return True is the members in the list are removed from the key's set.
	 */
	public boolean delSetStringByBatch(String key, List<String> member);
	public boolean delSetStringByBatch(String key, List<String> member,int index);
	
	/**
	 * delSetObjectByBatch
	 * @param key
	 * @param member
	 * @return True is the members in the list are removed from the key's set.
	 */
	public boolean delSetObjectByBatch(String key, List<Object> member);
	public boolean delSetObjectByBatch(String key, List<Object> member,int index);
	
	/**
	 * getAllSetString
	 * @param key
	 * @return The whole set by the key.
	 */
	public Set<String> getAllSetString(String key);
	public Set<String> getAllSetString(String key,int index);
	
	
	/**
	 * getAllSetObject
	 * @param key
	 * @return The whole set by the key.
	 */
	public Set<Object> getAllSetObject(String key);
	public Set<Object> getAllSetObject(String key,int index);
	
	/* zset */
	/**
	 * addSortedSetString
	 * @param key
	 * @param member
	 * @param score
	 * @param seconds
	 * @return True if the member is set to the key's sorted set with score and the key is expired by
	 * the specified seconds.
	 */
	public boolean addSortedSetString(String key, String member, double score, int seconds);
	public boolean addSortedSetString(String key, String member, double score, int seconds,int index);
	
	/**
	 * addSortedSetStringByBatch
	 * @param key
	 * @param members
	 * @param seconds
	 * @return True if the member/score pairs are set to the key's scored set and the key is expired by
	 * the specified seconds.
	 */
	public boolean addSortedSetStringByBatch(String key, Map<String, Double> members, int seconds);
	public boolean addSortedSetStringByBatch(String key, Map<String, Double> members, int seconds,int index);
	
	/**
	 * addSortedSetObject
	 * @param key
	 * @param member
	 * @param score
	 * @param seconds
	 * @return True if the member is set to the key's sorted set with score and the key is expired by
	 * the specified seconds.
	 */
	public boolean addSortedSetObject(String key, Object member, double score, int seconds);
	public boolean addSortedSetObject(String key, Object member, double score, int seconds,int index);
	
	/**
	 * addSortedSetObjectByBatch
	 * @param key
	 * @param members
	 * @param seconds
	 * @return True if the member/score pairs are set to the key's scored set and the key is expired by
	 * the specified seconds.
	 */
	public boolean addSortedSetObjectByBatch(String key, Map<Object, Double> members, int seconds);
	public boolean addSortedSetObjectByBatch(String key, Map<Object, Double> members, int seconds,int index);
	
	/**
	 * getSortedSetSize
	 * @param key
	 * @return The size of the key's sorted set.
	 */
	public long getSortedSetSize(String key);
	
	public long getSortedSetSize(String key,int index);
	
	/**
	 * incrSortedSetStringScore
	 * @param key
	 * @param member
	 * @param score
	 * @return The member's score in the keys's sorted set after the increment. 
	 * If exception thrown, return 0.
	 */
	public double incrSortedSetStringScore(String key, String member, double score);
	public double incrSortedSetStringScore(String key, String member, double score,int index);
	
	/**
	 * incrSortedSetObjectScore
	 * @param key
	 * @param member
	 * @param score
	 * @return The member's score in the keys's sorted set after the increment. 
	 * If exception thrown, return 0.
	 */
	public double incrSortedSetObjectScore(String key, Object member, double score);
	public double incrSortedSetObjectScore(String key, Object member, double score,int index);
	
	/**
	 * getSortedSetStringRange
	 * @param key
	 * @param start
	 * @param end
	 * @return The range of key's sorted set from start to end sorted by index.
	 */
	public Set<String> getSortedSetStringRange(String key, long start, long end);
	
	public Set<String> getSortedSetStringRange(String key, long start, long end,int index);
	
	/**
	 * getSortedSetObjectRange
	 * @param key
	 * @param start
	 * @param end
	 * @return The range of key's sorted set from start to end sorted by index.
	 */
	public Set<Object> getSortedSetObjectRange(String key, long start, long end);
	public Set<Object> getSortedSetObjectRange(String key, long start, long end,int index);
	
	/**
	 * getSortedSetStringRangeOrderByScore
	 * @param key
	 * @param start
	 * @param end
	 * @return  The range of key's sorted set from start to end sorted by member's score.
	 */
	public Set<String> getSortedSetStringRangeOrderByScore(String key, long start, long end);
	public Set<String> getSortedSetStringRangeOrderByScore(String key, long start, long end,int index);
	
	
	/**
	 * getSortedSetObjectRangeOrderByScore
	 * @param key
	 * @param start
	 * @param end
	 * @return The range of key's sorted set from start to end sorted by member's score.
	 */
	public Set<Object> getSortedSetObjectRangeOrderByScore(String key, long start, long end);
	public Set<Object> getSortedSetObjectRangeOrderByScore(String key, long start, long end,int index);

	/**
	 * delSortedSetString
	 * @param key
	 * @param member
	 * @return True if the member is removed from the key's sorted set.
	 */
	public boolean delSortedSetString(String key, String member);
	public boolean delSortedSetString(String key, String member,int index);
	
	
	/**
	 * delSortedSetObject
	 * @param key
	 * @param member
	 * @return True if the member is removed from the key's sorted set.
	 */
	public boolean delSortedSetObject(String key, Object member);
	public boolean delSortedSetObject(String key, Object member,int index);
	
	
	/**
	 * delSortedSetStringByBatch
	 * @param key
	 * @param member
	 * @return True if the members in the list are removed from the key's sorted set.
	 */
	public boolean delSortedSetStringByBatch(String key, List<String> member);
	public boolean delSortedSetStringByBatch(String key, List<String> member,int index);
	
	/**
	 * delSortedSetObjectByBatch
	 * @param key
	 * @param member
	 * @return True if the members in the list are removed from the key's sorted set.
	 */
	public boolean delSortedSetObjectByBatch(String key, List<Object> member);
	public boolean delSortedSetObjectByBatch(String key, List<Object> member,int index);
	
	public Long publish(final String channel, final String message);
	public void subscribe(final JedisPubSub jedisPubSub,final String key);
	
	public Map<String, String> hgetAll(String key);
	public Map<String, String> hgetAll(String key, int index);
	public Map<String, Object> getHashAllObject(String key);
	public Map<String, Object> getHashAllObject (String key, int index);
	
	public long decr(String key, int index);
	public long decr(String key);
	
	public long incrBy(String key, long integer);
	public long incrBy(String key, long integer,int index);
	public long decrBy(String key,long integer, int index);
	public long decrBy(String key,long integer);
	
}
