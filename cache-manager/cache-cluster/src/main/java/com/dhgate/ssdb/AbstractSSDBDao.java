package com.dhgate.ssdb;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.dhgate.memcache.IMemCached;
import com.dhgate.redis.AbstractRedisDao;
import com.dhgate.redis.ObjectBytesExchange;


/**
 * Created for extract common methods and unify the get/set, hget/hset use for String and Object.
 * 
 *
 * @author lidingkun
 *
 */
public abstract class AbstractSSDBDao implements ISSDBDao,IMemCached {
	public static final String EMPTY_STRING = "";
	protected int retryTime;
	protected long retryInterval;

	 private boolean isSlave = false;
	@Override
	public boolean setString(String key, String str, int liveSeconds) {
		if (key == null || str == null) {
			return false;
		}
		byte[] byteArray = null;
		try{
			byteArray = str.getBytes(AbstractRedisDao.DEFAULT_ENCODING);
		}catch(Exception e){
			e.printStackTrace();
		}
		if (null == byteArray) {
			return false;
		}
		return setBytes(key, byteArray, liveSeconds);
	}

	@Override
	public boolean setObject(String key, Object obj, int liveSeconds) {
		if (key == null || obj == null) {
			return false;
		}
		byte[] byteObj = ObjectBytesExchange.toByteArray(obj);
		if (null == byteObj) {
			return false;
		}
		return setBytes(key, byteObj, liveSeconds);
	}

	@Override
	public String getString(String key) {
		byte[] ret = getBytes(key);
		if (ret == null) {
			return null;
		}
		return new String(ret);
	}

	@Override
	public Object getObject(String key) {
		byte[] ret = getBytes(key);
		if (ret == null) {
			return null;
		}
		return ObjectBytesExchange.toObject(ret);
	}

	@Override
	public boolean hsetString(String hashName, String key, String str){
		if (key == null || str == null) {
			return false;
		}
		byte[] byteArray = null;
		try{
			byteArray = str.getBytes(AbstractRedisDao.DEFAULT_ENCODING);
		}catch(Exception e){
			e.printStackTrace();
		}
		if (null == byteArray) {
			return false;
		}
		return hsetBytes (hashName, key, byteArray);
	}
	
	@Override
	public boolean hsetObject(String hashName, String key, Object obj){
		if (key == null || obj == null) {
			return false;
		}
		byte[] byteObj = ObjectBytesExchange.toByteArray(obj);
		if (null == byteObj) {
			return false;
		}
		return hsetBytes(hashName, key, byteObj);
	}

	@Override
	public String hgetString(String hashName, String key){
		byte[] ret = hgetBytes(hashName, key);
		if (ret == null) {
			return null;
		}
		Object o = ObjectBytesExchange.toObject(ret);
		return (String)o;
	}

	@Override
	public Object hgetObject(String hashName, String key){
		byte[] ret = hgetBytes(hashName, key);
		if (ret == null) {
			return null;
		}
		return ObjectBytesExchange.toObject(ret);
	}
	
	@Override
	public Map<String, String> hgetallString(String hashName) {
		Map<String, String> ret = new HashMap<String, String>();
		if (hashName == null) {
			return ret;
		}

		Map<byte[], byte[]> temp = hgetallkvByte(hashName);
		for (byte[] b : temp.keySet()) {
			String key = new String(b);

			ret.put(key, new String(temp.get(b)));
		}

		return ret;
	}

	@Override
	public List<String> multiGeString(String[] keys) {
		List<byte[]> r = this.multiGetByte(keys);
		List<String> ret = new ArrayList<String>(r.size());
		for (int i=0;i < r.size();i++) {
			ret.add(new String(r.get(i)));
		}
		return ret;
	}
	
	@Override
	public List<Object> multiGet(String[] keys) {
		List<byte[]> tr = this.multiGetByte(keys);
		List<Object> ret = new ArrayList<Object>(tr.size());
		for (int i=0;i < tr.size();i++) {
			ret.add(ObjectBytesExchange.toObject(tr.get(i)));
		}
		return ret;
	}

	@Override
	public Map<String, byte[]> hgetallByte(String hashName) {
		Map<String, byte[]> ret = new HashMap<String, byte[]>();
		if (hashName == null) {
			return ret;
		}

		Map<byte[], byte[]> temp = hgetallkvByte(hashName);
		for (byte[] b : temp.keySet()) {
			String key = new String(b);

			ret.put(key, temp.get(b));
		}

		return ret;
	}
	
	@Override
	public Map<String, Object> hgetall(String hashName) {
		Map<String,Object> ret = new HashMap<String,Object> ();
		if (hashName == null) {
			return ret;
		}

		Map<byte[], byte[]> temp = hgetallkvByte (hashName);
		for (byte[] b : temp.keySet()) {
			String key = new String(b);
			Object v = ObjectBytesExchange.toObject(temp.get(b));
			ret.put(key, v);
		}
		return ret;
	}

	
	@Override
	public boolean set(String key, Object value) {
		return this.set(key, value,0);
	}

	@Override
	public boolean set(String key, Object value, Date expiry) {
		long time=(expiry.getTime()>System.currentTimeMillis())?(expiry.getTime()-System.currentTimeMillis()):expiry.getTime();
		if (key == null || value == null)
			return false;
		byte[] byteObj = ObjectBytesExchange.toByteArray(value);
		if (null == byteObj)
			return false;
		return setBytes(key, byteObj,(int) time/1000, 0);
	}

	@Override
	public boolean set(String key, Object value, long liveTime) {
		
		int exp=(int)liveTime;
		if (key == null || value == null)
			return false;
		byte[] byteObj = ObjectBytesExchange.toByteArray(value);
		if (null == byteObj)
			return false;
		return setBytes(key, byteObj, exp, 0);
		
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
		return setBytes(key, byteObj,(int) time/1000, 1);
	}

	@Override
	public boolean add(String key, Object value, long liveTime) {
		int exp=(int)liveTime;
		if (key == null || value == null)
			return false;
		byte[] byteObj = ObjectBytesExchange.toByteArray(value);
		if (null == byteObj)
			return false;
		return setBytes(key, byteObj, exp,1);
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
		return setBytes(key, byteObj,(int) time/1000, 0);
	}

	@Override
	public boolean replace(String key, Object value, long liveTime) {
		int exp=(int)liveTime;
		if (key == null || value == null)
			return false;
		byte[] byteObj = ObjectBytesExchange.toByteArray(value);
		if (null == byteObj)
			return false;
		return setBytes(key, byteObj, exp,2);
	}
	@Override
	public boolean flushAll() {
		return this.flushAll(null);
	}
	@Override
	public Object get(String key) {
		byte[] ret = getBytes(key);
		if (ret == null) {
			return null;
		}
		return ObjectBytesExchange.toObject(ret);
	}

	@Override
	public Map<String, byte[]> hScanByte(String hashName, String keyStart,String keyEnd, int limit) {
		Map<String, byte[]> ret = new HashMap<String, byte[]>();
		Map<byte[],byte[]> temp = this.hScanKVBytes(hashName, keyStart, keyEnd, limit);
		if (temp != null) {
			for (byte [] b:temp.keySet()) {
				String key = new String(b);	
				ret.put(key, temp.get(b));
			}
		}
		
		temp = null;
		return ret;
	}
	
	@Override
	public Map<String, String> hScanString(String hashName, String keyStart,String keyEnd, int limit) {
		Map<String,String> ret = new HashMap<String, String>();
		Map<byte[],byte[]> temp = this.hScanKVBytes(hashName, keyStart, keyEnd, limit);
		if (temp != null) {
			for (byte [] b:temp.keySet()) {
				String key = new String(b);	
				ret.put(key, new String(temp.get(b)));
			}
		}
		
		temp = null;
		return ret;
	}
	
	@Override
	public Map<String, Object> hScan(String hashName, String keyStart,String keyEnd, int limit) {
		Map<String,Object> ret = new HashMap<String,Object> ();
		if (hashName == null) {
			return ret;
		}
		
		Map<byte[],byte[]> temp = this.hScanKVBytes(hashName, keyStart, keyEnd, limit);
		if (temp != null) {
			for (byte [] b:temp.keySet()) {
				String key = new String(b);	
				Object v = ObjectBytesExchange.toObject(temp.get(b));
				ret.put(key, v);
			}
		}
		
		temp = null;
		return ret;
	}
	@Override
	public Map<String, String> multiHgetString(String hashName, String[] keys) {
		Map<String, String> ret = new HashMap<String, String>();
		if (hashName == null) {
			return ret;
		}
		Map<byte[], byte[]> temp = this.multiHgetKVBytes(hashName, keys);
		if (temp != null) {

			for (byte[] b : temp.keySet()) {
				String key = new String(b);

				ret.put(key, new String(temp.get(b)));
			}
		}
		temp = null;
		return ret;
	}
	
	@Override
	public Map<String, byte[]> multiHgetByte(String hashName, String[] keys) {
		Map<String,byte[]> ret = new HashMap<String,byte[]> ();
		if (hashName == null) {
			return ret;
		}
		
		Map<byte[], byte[]> temp = this.multiHgetKVBytes(hashName, keys);
		if (temp != null) {

			for (byte[] b : temp.keySet()) {
				String key = new String(b);

				ret.put(key, temp.get(b));
			}
		}
		
		temp = null;
		return ret;
	}
	
	@Override
	public Map<String, Object> multiHget (String hashName, String[] keys) {
		Map<String,Object> ret = new HashMap<String,Object> ();
		if (hashName == null) {
			return ret;
		}
		
		Map<byte[], byte[]> temp = this.multiHgetKVBytes(hashName, keys);
		if (temp != null) {

			for (byte[] b : temp.keySet()) {
				String key = new String(b);
				Object v = ObjectBytesExchange.toObject(temp.get(b));
				ret.put(key, v);
			}
		}
		
		temp = null;
		return ret;
	}
	@Override
	public boolean multiSetByte(Map<String, byte[]> map) {
		Map<byte[],byte[]> temp = new HashMap<byte[], byte[]>();
		
		for (String key : map.keySet()) {
			try {
				temp.put(key.getBytes(AbstractRedisDao.DEFAULT_ENCODING),map.get(key));
				//list.add(ObjectBytesExchange.toByteArray(map.get(key)));
			} catch (UnsupportedEncodingException e) {
				throw new RuntimeException("Error occurred when multiSet ", e);
			}		
		}
		return this.multiSetKVByte(temp);
	}
	
	@Override
	public boolean multiSetString(Map<String, String> map) {
		Map<byte[],byte[]> temp = new HashMap<byte[], byte[]>();
		
		for (String key : map.keySet()) {
			try {
				temp.put(key.getBytes(AbstractRedisDao.DEFAULT_ENCODING),map.get(key).getBytes(AbstractRedisDao.DEFAULT_ENCODING));
				//list.add(ObjectBytesExchange.toByteArray(map.get(key)));
			} catch (UnsupportedEncodingException e) {
				throw new RuntimeException("Error occurred when multiSet ", e);
			}		
		}
		return this.multiSetKVByte(temp);
	}
	
	@Override
	public boolean multiSet(Map<String, Object> map) {
		Map<byte[],byte[]> temp = new HashMap<byte[], byte[]>();	
		for (String key : map.keySet()) {
			try {
				temp.put(key.getBytes(AbstractRedisDao.DEFAULT_ENCODING),ObjectBytesExchange.toByteArray(map.get(key)));
			} catch (UnsupportedEncodingException e) {
				throw new RuntimeException("Error occurred when multiSet ", e);
			}		
		}
		return this.multiSetKVByte(temp);
	}
	@Override
	public boolean multiHsetByte(String hashName, Map<String, byte[]> map) {
		Map<byte[],byte[]> temp = new HashMap<byte[], byte[]>();	
		for (String key : map.keySet()) {
			try {
				temp.put(key.getBytes(AbstractRedisDao.DEFAULT_ENCODING),map.get(key));
			} catch (UnsupportedEncodingException e) {
				throw new RuntimeException("Error occurred when multiSet ", e);
			}		
		}
		return this.multiHsetKVbytes (hashName,temp);
	}
	
	@Override
	public boolean multiHsetString(String hashName, Map<String, String> map) {
		Map<byte[],byte[]> temp = new HashMap<byte[], byte[]>();	
		for (String key : map.keySet()) {
			try {
				temp.put(key.getBytes(AbstractRedisDao.DEFAULT_ENCODING),map.get(key).getBytes(AbstractRedisDao.DEFAULT_ENCODING));
			} catch (UnsupportedEncodingException e) {
				throw new RuntimeException("Error occurred when multiSet ", e);
			}		
		}
	
		return this.multiHsetKVbytes (hashName,temp);
	}

	@Override
	public boolean multiHset(String hashName, Map<String, Object> map) {
		Map<byte[],byte[]> temp = new HashMap<byte[], byte[]>();	
		for (String key : map.keySet()) {
			try {
				temp.put(key.getBytes(AbstractRedisDao.DEFAULT_ENCODING),ObjectBytesExchange.toByteArray(map.get(key)));
			} catch (UnsupportedEncodingException e) {
				throw new RuntimeException("Error occurred when multiSet ", e);
			}		
		}
	
		return this.multiHsetKVbytes (hashName,temp);
	}
	
	protected abstract boolean multiHsetKVbytes (String hashName, Map<byte[], byte[]> map);
	protected abstract boolean multiSetKVByte(Map<byte[], byte[]> map);
	protected abstract Map<byte[], byte[]> multiHgetKVBytes(String hashName, String[] keys);
	protected abstract Map<byte[], byte[]> hgetallkvByte (String hashName);
	protected abstract boolean setBytes(String key, byte[] bytes, int seconds,int flag);
	protected abstract Map<byte[], byte[]> hScanKVBytes(String hashName, String keyStart,String keyEnd, int limit);
	public abstract boolean setBytes(String key, byte[] bytes, int liveSeconds);

	public abstract byte[] getBytes(String key);
	
	public abstract boolean hsetBytes(String hashName, String key, byte[] bytes);

	public abstract byte[] hgetBytes(String hashName, String key);

	public boolean isSlave() {
		return isSlave;
	}

	public void setSlave(boolean isSlave) {
		this.isSlave = isSlave;
	}
}