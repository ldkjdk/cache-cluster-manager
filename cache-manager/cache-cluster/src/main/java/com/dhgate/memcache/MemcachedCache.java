package com.dhgate.memcache;

import java.util.Date;
import java.util.Map;

import com.dhgate.ICachedConfigManager;
import com.dhgate.memcache.core.MemCachedClient;
import com.dhgate.memcache.schooner.SchoonerSockIOPool;

/**
 * 
 * @author lidingkun
 *
 */

public class MemcachedCache implements IMemCached {

	private int weight =1;

    private MemCachedClient   memCachedClient;

    private String            name;

    private ICachedConfigManager cacheManager;
    private boolean isSlave = false;
    public MemcachedCache(MemCachedClient hedClimemCachedClientent){
        this.memCachedClient = hedClimemCachedClientent;
    }

    public boolean add(String key, Object value) {
        return memCachedClient.add(key, value);
    }

    // public boolean add(String key, Object value, Integer hashCode) {
    // return memCachedClient.add(key, value, hashCode);
    // }

    public boolean add(String key, Object value, Date expiry) {
        return memCachedClient.add(key, value, expiry);
    }

    // public boolean add(String key, Object value, Date expiry, Integer
    // hashCode) {
    //
    // return memCachedClient.add(key, value, hashCode);
    // }

    /*
     * public long addOrDecr(String key) { return memCachedClient.addOrDecr(key); } public long addOrDecr(String key,
     * long inc) { return memCachedClient.addOrDecr(key, inc); } public long addOrDecr(String key, long inc, Integer
     * hashCode) { return memCachedClient.addOrDecr(key, inc, hashCode); } public long addOrIncr(String key) { return
     * memCachedClient.addOrIncr(key); } public long addOrIncr(String key, long inc) { return
     * memCachedClient.addOrIncr(key, inc); } public long addOrIncr(String key, long inc, Integer hashCode) { return
     * memCachedClient.addOrIncr(key, inc, hashCode); } public boolean append(String key, Object value, Integer
     * hashCode) { return memCachedClient.append(key, value, hashCode); }
     */
    // public boolean append(String key, Object value) {
    // return memCachedClient.append(key, value);
    // }

    /*
     * public boolean cas(String key, Object value, Integer hashCode, long casUnique) { return memCachedClient.cas(key,
     * value, hashCode, casUnique); } public boolean cas(String key, Object value, Date expiry, long casUnique) { return
     * memCachedClient.cas(key, value, expiry, casUnique); } public boolean cas(String key, Object value, Date expiry,
     * Integer hashCode, long casUnique) { return memCachedClient.cas(key, value, expiry, hashCode, casUnique); } public
     * boolean cas(String key, Object value, long casUnique) { return memCachedClient.cas(key, value, casUnique); }
     */

    // public long decr(String key) {
    // return memCachedClient.decr(key);
    // }
    //
    // public long decr(String key, long inc) {
    // return memCachedClient.decr(key, inc);
    // }

    // public long decr(String key, long inc, Integer hashCode) {
    // return memCachedClient.decr(key, inc, hashCode);
    // }

    public boolean delete(String key) {
        return memCachedClient.delete(key);
    }

    // public boolean delete(String key, Date expiry) {
    // return memCachedClient.delete(key, expiry);
    // }

    // public boolean delete(String key, Integer hashCode, Date expiry) {
    // return memCachedClient.delete(key, hashCode, expiry);
    // }

    /*
     * public boolean flushAll() { return memCachedClient.flushAll(); } public boolean flushAll(String[] servers) {
     * return memCachedClient.flushAll(servers); }
     */

    public Object get(String key) {
        return memCachedClient.get(key);
    }

    /*
     * public Object get(String key, Integer hashCode) { return memCachedClient.get(key, hashCode); } public Object
     * get(String key, Integer hashCode, boolean asString) { return memCachedClient.get(key, hashCode, asString); }
     */

    /*
     * public long getCounter(String key) { return memCachedClient.getCounter(key); } public long getCounter(String key,
     * Integer hashCode) { return memCachedClient.getCounter(key, hashCode); }
     */

    public Map<String, Object> getMulti(String[] keys) {
        return memCachedClient.getMulti(keys);
    }

    /*
     * public Map<String, Object> getMulti(String[] keys, Integer[] hashCodes) { return memCachedClient.getMulti(keys,
     * hashCodes); } public Map<String, Object> getMulti(String[] keys, Integer[] hashCodes, boolean asString) { return
     * memCachedClient.getMulti(keys, hashCodes, asString); }
     */
    public Object[] getMultiArray(String[] keys) {
        return memCachedClient.getMultiArray(keys);
    }

    /*
     * public Object[] getMultiArray(String[] keys, Integer[] hashCodes) { return memCachedClient.getMultiArray(keys,
     * hashCodes); } public Object[] getMultiArray(String[] keys, Integer[] hashCodes, boolean asString) { return
     * memCachedClient.getMultiArray(keys, hashCodes, asString); } public MemcachedItem gets(String key) { return
     * memCachedClient.gets(key); } public MemcachedItem gets(String key, Integer hashCode) { return
     * memCachedClient.gets(key, hashCode); }
     */
    // public long incr(String key) {
    // return memCachedClient.incr(key);
    // }
    //
    // public long incr(String key, long inc) {
    // return memCachedClient.incr(key, inc);
    // }

    /*
     * public long incr(String key, long inc, Integer hashCode) { return memCachedClient.incr(key, inc, hashCode); }
     */

    public boolean keyExists(String key) {
        return memCachedClient.keyExists(key);
    }

    // public boolean prepend(String key, Object value, Integer hashCode) {
    // return memCachedClient.prepend(key, value, hashCode);
    // }

    // public boolean prepend(String key, Object value) {
    // return memCachedClient.prepend(key, value);
    // }

    public boolean replace(String key, Object value) {
        return memCachedClient.replace(key, value);
    }

    // public boolean replace(String key, Object value, Integer hashCode) {
    // return memCachedClient.replace(key, value, hashCode);
    // }

    public boolean replace(String key, Object value, Date expiry) {
        return memCachedClient.replace(key, value, expiry);
    }

    // public boolean replace(String key, Object value, Date expiry, Integer
    // hashCode) {
    // return memCachedClient.replace(key, value, hashCode);
    // }

    public boolean set(String key, Object value) {
        return memCachedClient.set(key, value);
    }

    // public boolean set(String key, Object value, Integer hashCode) {
    // return memCachedClient.set(key, value, hashCode);
    // }

    public boolean set(String key, Object value, Date expiry) {
        return memCachedClient.set(key, value, expiry);
    }

    // public boolean set(String key, Object value, Date expiry, Integer
    // hashCode) {
    // return memCachedClient.set(key, value, hashCode);
    // }

    /*
     * public void setClassLoader(ClassLoader classLoader) { memCachedClient.setClassLoader(classLoader); } public void
     * setCompressEnable(boolean compressEnable) { memCachedClient.setCompressEnable(compressEnable); } public void
     * setCompressThreshold(long compressThreshold) { memCachedClient.setCompressThreshold(compressThreshold); } public
     * void setDefaultEncoding(String defaultEncoding) { memCachedClient.setDefaultEncoding(defaultEncoding); } public
     * void setErrorHandler(ErrorHandler errorHandler) { memCachedClient.setErrorHandler(errorHandler); } public void
     * setPrimitiveAsString(boolean primitiveAsString) { memCachedClient.setPrimitiveAsString(primitiveAsString); }
     * public void setSanitizeKeys(boolean sanitizeKeys) { memCachedClient.setSanitizeKeys(sanitizeKeys); } public void
     * setTransCoder(TransCoder transCoder) { memCachedClient.setTransCoder(transCoder); } public Map<String,
     * Map<String, String>> stats() { return memCachedClient.stats(); } public Map<String, Map<String, String>>
     * stats(String[] servers) { return memCachedClient.stats(servers); } public Map<String, Map<String, String>>
     * statsCacheDump(int slabNumber, int limit) { return memCachedClient.statsCacheDump(slabNumber, limit); } public
     * Map<String, Map<String, String>> statsCacheDump(String[] servers, int slabNumber, int limit) { return
     * memCachedClient.statsCacheDump(servers, slabNumber, limit); } public Map<String, Map<String, String>>
     * statsItems() { return memCachedClient.statsItems(); } public Map<String, Map<String, String>> statsItems(String[]
     * servers) { return memCachedClient.statsItems(servers); } public Map<String, Map<String, String>> statsSlabs() {
     * return memCachedClient.statsSlabs(); } public Map<String, Map<String, String>> statsSlabs(String[] servers) {
     * return memCachedClient.statsSlabs(servers); } public boolean storeCounter(String key, long counter) { return
     * memCachedClient.storeCounter(key, counter); } public boolean storeCounter(String key, Long counter) { return
     * memCachedClient.storeCounter(key, counter); } public boolean storeCounter(String key, Long counter, Integer
     * hashCode) { return memCachedClient.storeCounter(key, counter, hashCode); } public boolean sync(String key,
     * Integer hashCode) { return memCachedClient.sync(key, hashCode); } public boolean sync(String key) { return
     * memCachedClient.sync(key); } public boolean syncAll() { return memCachedClient.syncAll(); } public boolean
     * syncAll(String[] servers) { return memCachedClient.syncAll(servers); }
     */
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public ICachedConfigManager getCacheManager() {
        return cacheManager;
    }

    protected void setCacheManager(ICachedConfigManager cacheManager) {
        this.cacheManager = cacheManager;
    }

    public boolean add(String key, Object value, long liveTime) {
        return add(key, value, new Date(liveTime * 1000));
    }

    // public boolean delete(String key, long liveTime) {
    // return delete(key, new Date(liveTime * 60 * 1000));
    // }

    public boolean replace(String key, Object value, long liveTime) {
        return replace(key, value, new Date(liveTime * 1000));
    }

    public boolean set(String key, Object value, long liveTime) {
        return set(key, value, new Date(liveTime * 1000));
    }

	public MemCachedClient getMemCachedClient() {
		return memCachedClient;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		if (memCachedClient != null) {
			memCachedClient.close();
		}
		memCachedClient=null;
	}

	@Override
	public int getWeight() {
		
		return this.weight;
	}

	@Override
	public boolean pingCheck() {
		SchoonerSockIOPool pool = SchoonerSockIOPool.getInstance(this.getName());
		if (pool == null) {
			return false;
		}
		try {
			return pool.ping();
		} catch (Exception e) {
			return false;
		}
	}

	public void setWeight(int weight) {
		this.weight = weight;
	}
	
	public boolean isSlave() {
		return isSlave;
	}

	public void setSlave(boolean isSlave) {
		this.isSlave = isSlave;
	}

	@Override
	public boolean flushAll() {
		return memCachedClient.flushAll();
	}

	@Override
	public boolean flushAll(String[] servers) {
		return memCachedClient.flushAll(servers);
	}
}
