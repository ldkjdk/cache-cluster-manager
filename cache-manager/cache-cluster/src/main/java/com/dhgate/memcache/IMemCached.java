/**
 * 
 */
package com.dhgate.memcache;

import java.util.Date;
import java.util.Map;

import com.dhgate.ICachedConfigManager;

/**
 * Memcached interface
 * 
 * @author lidingkun
 */
public interface IMemCached {

    
    public ICachedConfigManager getCacheManager();

    /**
     * Checks to see if key exists in cache.
     * 
     * @param key the key to look for
     * @return true if key found in cache, false if not (or if cache is down)
     */
    public boolean keyExists(String key);

    /**
     * Deletes an object from cache given cache key.
     * 
     * @param key the key to be removed
     * @return <code>true</code>, if the data was deleted successfully
     */
    public boolean delete(String key);

    /**
     * Deletes an object from cache given cache key and expiration date.
     * 
     * @param key the key to be removed
     * @param expiry when to expire the record.
     * @return <code>true</code>, if the data was deleted successfully
     */
    // public boolean delete(String key, Date expiry);

    /**
     * Deletes an object from cache given cache key and expiration date.
     * 
     * @param key the key to be removed
     * @param liveTime 服务器在liveTime分钟后删除 单位为分钟
     * @return <code>true</code>, if the data was deleted successfully
     */
    // public boolean delete(String key, long liveTime);

    /**
     * Stores data on the server; only the key and the value are specified.
     * 
     * @param key key to store data under
     * @param value value to store
     * @return true, if the data was successfully stored
     */
    public boolean set(String key, Object value);

    /**
     * Stores data on the server; the key, value, and an expiration time are specified.
     * 
     * @param key key to store data under
     * @param value value to store
     * @param expiry when to expire the record
     * @return true, if the data was successfully stored
     */
    public boolean set(String key, Object value, Date expiry);

    /**
     * Stores data on the server; the key, value, and an expiration time are specified.
     * 
     * @param key key to store data under
     * @param value value to store
     * @param liveTime,TimeUnit second
     * @return true, if the data was successfully stored
     */
    public boolean set(String key, Object value, long liveTime);

    /**
     * Adds data to the server; only the key and the value are specified.
     * 
     * @param key key to store data under
     * @param value value to store
     * @return true, if the data was successfully stored
     */
    public boolean add(String key, Object value);

    /**
     * Adds data to the server; the key, value, and an expiration time are specified.
     * 
     * @param key key to store data under
     * @param value value to store
     * @param expiry when to expire the record
     * @return true, if the data was successfully stored
     */
    public boolean add(String key, Object value, Date expiry);

    /**
     * Adds data to the server; the key, value, and an expiration time are specified.
     * 
     * @param key key to store data under
     * @param value value to store
     * @param liveTime,TimeUnit SECONDS
     * @return true, if the data was successfully stored
     */
    public boolean add(String key, Object value, long liveTime);

    /**
     * Updates data on the server; only the key and the value are specified.
     * 
     * @param key key to store data under
     * @param value value to store
     * @return true, if the data was successfully stored
     */
    public boolean replace(String key, Object value);

    /**
     * Updates data on the server; the key, value, and an expiration time are specified.
     * 
     * @param key key to store data under
     * @param value value to store
     * @param expiry when to expire the record
     * @return true, if the data was successfully stored
     */
    public boolean replace(String key, Object value, Date expiry);

    /**
     * Updates data on the server; the key, value, and an expiration time are specified.
     * 
     * @param key key to store data under
     * @param value value to store
     * @param liveTime 缓存时间 单位为分钟
     * @return true, if the data was successfully stored
     */
    public boolean replace(String key, Object value, long liveTime);

    /**
     * Increment the value at the specified key by 1, and then return it.
     * 
     * @param key key where the data is stored
     * @return -1, if the key is not found, the value after incrementing otherwise
     */
    // public long incr(String key);

    /**
     * Increment the value at the specified key by passed in val.
     * 
     * @param key key where the data is stored
     * @param inc how much to increment by
     * @return -1, if the key is not found, the value after incrementing otherwise
     */
    // public long incr(String key, long inc);

    /**
     * Decrement the value at the specified key by 1, and then return it.
     * 
     * @param key key where the data is stored
     * @return -1, if the key is not found, the value after incrementing otherwise
     */
    // public long decr(String key);

    /**
     * Decrement the value at the specified key by passed in value, and then return it.
     * 
     * @param key key where the data is stored
     * @param inc how much to increment by
     * @return -1, if the key is not found, the value after incrementing otherwise
     */
    // public long decr(String key, long inc);

    /**
     * Retrieve a key from the server, using a specific hash. If the data was compressed or serialized when compressed,
     * it will automatically<br/>
     * be decompressed or serialized, as appropriate. (Inclusive or)<br/>
     * <br/>
     * Non-serialized data will be returned as a string, so explicit conversion to<br/>
     * numeric types will be necessary, if desired<br/>
     * 
     * @param key key where data is stored
     * @return the object that was previously stored, or null if it was not previously stored
     */
    public Object get(String key);

    /**
     * Retrieve multiple objects from the memcache. This is recommended over repeated calls to {@link #get(String)
     * get()}, since it<br/>
     * is more efficient.<br/>
     * 
     * @param keys String array of keys to retrieve
     * @return Object array ordered in same order as key array containing results
     */
    public Object[] getMultiArray(String[] keys);
    public boolean flushAll();

	public boolean flushAll(String[] servers);
    
    /**
     * Retrieve multiple objects from the memcache. This is recommended over repeated calls to {@link #get(String)
     * get()}, since it<br/>
     * is more efficient.<br/>
     * 
     * @param keys String array of keys to retrieve
     * @return a hashmap with entries for each key is found by the server, keys that are not found are not entered into
     * the hashmap, but attempting to retrieve them from the hashmap gives you null.
     */
    public Map<String, Object> getMulti(String[] keys);

    // public boolean append(String key, Object value);

    // public boolean prepend(String key, Object value);
    public String getName();
    public int getWeight();
    public boolean pingCheck() ;
    public void setName(String name);
    public void setWeight(int weight);
    public void close();
    public boolean isSlave();
    public void setSlave(boolean isSlave);
    
}
