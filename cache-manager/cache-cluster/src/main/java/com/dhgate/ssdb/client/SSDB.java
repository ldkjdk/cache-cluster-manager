package com.dhgate.ssdb.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.dhgate.redis.AbstractRedisDao;
import com.dhgate.ssdb.SSDBPool;



/**
 * Make some change since 1.0.0 based on origin java client.
 * 	create ping, setx, isExists methods invocation.
 * 	modify the return value type to boolen in some methods.
 * 	Use UTF-8 when using String.getBytes().
 */
public class SSDB {
	public Link link;
	private SSDBPool pool;

	public SSDB(String host, int port) throws Exception {
		this(host, port, 0);
	}

	public SSDB(String host, int port, int timeout_ms) throws Exception {
		link = new Link(host, port, timeout_ms);
	}

	public void close() throws Exception {
		link.close();
	}

	public Response request(String cmd, byte[]... params) throws Exception {
		return link.request(cmd, params);
	}

	public Response request(String cmd, String... params) throws Exception {
		return link.request(cmd, params);
	}

	public Response request(String cmd, List<byte[]> params) throws Exception {
		return link.request(cmd, params);
	}

	/* kv */

	public boolean ping() throws Exception {

		Response resp = link.request("ping");
		if (resp.ok()) {
			return true;
		}

		return false;
	}

	//There is no quit command in ssdb.
//	public void quit() throws Exception {
//		Response resp = link.request("quit");
//		if (resp.ok()) {
//			return;
//		}
//		resp.exception();
//	}

	public boolean setex(String key, byte[] val, int liveSeconds)
			throws Exception {
		return setex(key.getBytes(AbstractRedisDao.DEFAULT_ENCODING), val, 
				String.valueOf(liveSeconds).getBytes(AbstractRedisDao.DEFAULT_ENCODING));
	}

	private boolean setex(byte[] key, byte[] val, byte[] liveSeconds)
			throws Exception {
		Response resp = link.request("setx", key, val, liveSeconds);
		if (!resp.ok()) {
			resp.exception();
		}
		return true;
	}

	
	public boolean setnx (String key, byte[] val, int liveSeconds)
			throws Exception {
		return setnx(key.getBytes(AbstractRedisDao.DEFAULT_ENCODING), val, 
				String.valueOf(liveSeconds).getBytes(AbstractRedisDao.DEFAULT_ENCODING));
	}
	
	private boolean setnx(byte[] key, byte[] val, byte[] liveSeconds)
			throws Exception {
		Response resp = link.request("setnx", key, val, liveSeconds);
		if (!resp.ok()) {
			resp.exception();
		}
		return true;
	}
	
	public boolean isExist(String key) throws Exception {
		return isExist(key.getBytes(AbstractRedisDao.DEFAULT_ENCODING));
	}

	private boolean isExist(byte[] key) throws Exception {
		Response resp = link.request("exists", key);
		if (!resp.ok()) {
			resp.exception();
		}
		if (resp.raw.size() != 2) {
			throw new Exception("Invalid response");
		}
		if (resp.raw.get(1)[0] == '1') {
			return true;
		} else {
			return false;
		}
	}
	
	public long incr(String key, long by) throws Exception {
		return incr(key.getBytes(AbstractRedisDao.DEFAULT_ENCODING), by);
	}
	
	private long incr(byte[] key, long by) throws Exception {
		Response resp = link.request("incr", key, String.valueOf(by).getBytes(AbstractRedisDao.DEFAULT_ENCODING));
		if (!resp.ok()) {
			resp.exception();
		}
		if (resp.raw.size() != 2) {
			throw new Exception("Invalid response");
		}
		return Long.parseLong(new String(resp.raw.get(1)));
	}
	
	public long decr(String key, long by) throws Exception{
		return decr(key.getBytes(AbstractRedisDao.DEFAULT_ENCODING), by);
	}
	
	private long decr(byte[] key, long by) throws Exception {
		Response resp = link.request("decr", key, String.valueOf(by).getBytes(AbstractRedisDao.DEFAULT_ENCODING));
		if (!resp.ok()) {
			resp.exception();
		}
		if (resp.raw.size() != 2) {
			throw new Exception("Invalid response");
		}
		return Long.parseLong(new String(resp.raw.get(1)));
	}

	public boolean expire(String key, int liveSeconds) throws Exception {
		return expire(key.getBytes(AbstractRedisDao.DEFAULT_ENCODING), liveSeconds);
	}

	private boolean expire(byte[] key, int liveSeconds) throws Exception {
		Response resp = link.request("expire", key, String.valueOf(liveSeconds)
				.getBytes(AbstractRedisDao.DEFAULT_ENCODING));
		if (!resp.ok()) {
			resp.exception();
		}
		if (resp.raw.size() != 2) {
			throw new Exception("Invalid response");
		}
		if (resp.raw.get(1)[0] == '1') {
			return true;
		} else {
			return false;
		}
	}
	
	public long ttl(String key) throws Exception {
		return ttl(key.getBytes(AbstractRedisDao.DEFAULT_ENCODING));
	}

	private long ttl(byte[] key) throws Exception {
		Response resp = link.request("ttl", key);
		if (!resp.ok()) {
			resp.exception();
		}	
		if (resp.raw.size() != 2) {
			throw new Exception("Invalid response");
		}
		return Long.parseLong(new String(resp.raw.get(1)));
	}

	private boolean set(byte[] key, byte[] val) throws Exception {
		Response resp = link.request("set", key, val);
		if (!resp.ok()) {
			resp.exception();
		}
		return true;
	}

	public boolean set(String key, byte[] val) throws Exception {
		return set(key.getBytes(AbstractRedisDao.DEFAULT_ENCODING), val);
	}

	public boolean set(String key, String val) throws Exception {
		return set(key, val.getBytes(AbstractRedisDao.DEFAULT_ENCODING));
	}

	private boolean del(byte[] key) throws Exception {
		Response resp = link.request("del", key);
		if (!resp.ok()) {
			resp.exception();
		}
		return true;
	}

	public boolean del(String key) throws Exception {
		return del(key.getBytes(AbstractRedisDao.DEFAULT_ENCODING));
	}

	/***
	 * 
	 * @param key
	 * @return null if not found
	 * @throws Exception
	 */
	private byte[] get(byte[] key) throws Exception {
		Response resp = link.request("get", key);
		if (resp.not_found()) {
			return null;
		}
		if (!resp.ok()) {
			resp.exception();
		}
		if (resp.raw.size() != 2) {
			throw new Exception("Invalid response");
		}
		return resp.raw.get(1);
	}

	/***
	 * 
	 * @param key
	 * @return null if not found
	 * @throws Exception
	 */
	public byte[] get(String key) throws Exception {
		return get(key.getBytes(AbstractRedisDao.DEFAULT_ENCODING));
	}
	
	
	public  List<String> keys (String key_start, String key_end,int limit,List<String> r) throws Exception {
		if (key_start == null) {
			key_start = "";
		}
		if (key_end == null) {
			key_end = "";
		}
		Response resp = link.request("keys", key_start, key_end, (new Integer(limit)).toString());
		if (!resp.ok()) {
			resp.exception();
		}
		
		if (!resp.ok()) {
			resp.exception();
		}
		
		for (int i=1;i < resp.raw.size();i++) {
			r.add(new String(resp.raw.get(i)));
		}
		return r;
	}
	
	public  List<String> HKeys (String hashName,String key_start, String key_end,int limit) throws Exception {
		List<String> r = new ArrayList<String>();
		if (key_start == null) {
			key_start = "";
		}
		if (key_end == null) {
			key_end = "";
		}
		Response resp = link.request("hkeys", hashName,key_start, key_end, (new Integer(limit)).toString());
		if (!resp.ok()) {
			resp.exception();
		}
		
		if (!resp.ok()) {
			resp.exception();
		}
		
		for (int i=1;i < resp.raw.size();i++) {
			r.add(new String(resp.raw.get(i)));
		}
		return r;
	}
	

	private Response _scan(String cmd, String key_start, String key_end,int limit) throws Exception {
		if (key_start == null) {
			key_start = "";
		}
		if (key_end == null) {
			key_end = "";
		}
		Response resp = link.request(cmd, key_start, key_end, (new Integer(
				limit)).toString());
		if (!resp.ok()) {
			resp.exception();
		}
		resp.buildMap();
		return resp;
	}

	private Response scan(String key_start, String key_end, int limit)throws Exception {
		return _scan("scan", key_start, key_end, limit);
	}

	private Response rscan(String key_start, String key_end, int limit) throws Exception {
		return _scan("rscan", key_start, key_end, limit);
	}

	/* hashmap */

	private boolean hset(String name, byte[] key, byte[] val) throws Exception {
		Response resp = link.request("hset", name.getBytes(AbstractRedisDao.DEFAULT_ENCODING), key, val);
		if (!resp.ok()) {
			resp.exception();
		}
		return true;
	}

	public boolean hset(String name, String key, byte[] val) throws Exception {
		return this.hset(name, key.getBytes(AbstractRedisDao.DEFAULT_ENCODING), val);
	}

	public boolean hset(String name, String key, String val) throws Exception {
		return this.hset(name, key, val.getBytes(AbstractRedisDao.DEFAULT_ENCODING));
	}

	private boolean hdel(String name, byte[] key) throws Exception {
		Response resp = link.request("hdel", name.getBytes(AbstractRedisDao.DEFAULT_ENCODING), key);
		if (!resp.ok()) {
			resp.exception();
		}	
		return true;
	}

	public boolean hdel(String name, String key) throws Exception {
		return this.hdel(name, key.getBytes(AbstractRedisDao.DEFAULT_ENCODING));
	}
	
	public long hsize(String name) throws Exception {
		Response resp = link.request("hsize", name.getBytes(AbstractRedisDao.DEFAULT_ENCODING));
		if (!resp.ok()) {
			resp.exception();
		}
		return Long.parseLong(new String(resp.raw.get(1)));
	}

	public boolean hclear(String name) throws Exception {
		Response resp = link.request("hclear", name.getBytes(AbstractRedisDao.DEFAULT_ENCODING));
		if (!resp.ok()) {
			resp.exception();
		}
		return true;
	}

	/**
	 * 
	 * @param name
	 * @param key
	 * @return null if not found
	 * @throws Exception
	 */
	private byte[] hget(String name, byte[] key) throws Exception {
		Response resp = link.request("hget", name.getBytes(AbstractRedisDao.DEFAULT_ENCODING), key);
		if (resp.not_found()) {
			return null;
		}
		if (!resp.ok()) {
			resp.exception();
		}
		if (resp.raw.size() != 2) {
			throw new Exception("Invalid response");
		}	
		return resp.raw.get(1);
	}

	
	private Map<byte[], byte[]> hgetAll (byte [] name) throws Exception {
		Response resp = link.request("hgetall", name);
		if (resp.not_found()) {
			return null;
		}
		if (!resp.ok()) {
			resp.exception();
		}
		if (resp.raw.size() < 2) {
			throw new Exception("Invalid response");
		}	
		resp.buildMap();
		return resp.items;
	}
	
	/**
	 * 
	 * @param name
	 * @param key
	 * @return null if not found
	 * @throws Exception
	 */
	public byte[] hget(String name, String key) throws Exception {
		return hget(name, key.getBytes(AbstractRedisDao.DEFAULT_ENCODING));
	}
	
	/**
	 * 
	 * @param name
	 * @return null if not found
	 * @throws Exception
	 */
	public Map<byte[], byte[]> hgetAll(String name) throws Exception {
		return hgetAll (name.getBytes(AbstractRedisDao.DEFAULT_ENCODING));
		
	}

	private Response hscan(String cmd, String name, String key_start,
			String key_end, int limit) throws Exception {
		if (key_start == null) {
			key_start = "";
		}
		if (key_end == null) {
			key_end = "";
		}
		Response resp = link.request(cmd, name, key_start, key_end,
				(new Integer(limit)).toString());
		if (!resp.ok()) {
			resp.exception();
		}
		for (int i = 1; i < resp.raw.size(); i += 2) {
			byte[] k = resp.raw.get(i);
			byte[] v = resp.raw.get(i + 1);
			resp.keys.add(k);
			resp.items.put(k, v);
		}
		return resp;
	}

	public Map<byte[], byte[]> hscan(String hashname, String keyStart, String keyEnd,int limit) throws Exception {
		Response rep = hscan("hscan", hashname, keyStart, keyEnd, limit);
		return rep.items;
	}

	public Map<byte[], byte[]> hrscan(String name, String key_start, String key_end,
			int limit) throws Exception {
		Response rep = hscan("hrscan", name, key_start, key_end, limit);
		return rep.items;
	}

	private long hincr(String name, String key, long by) throws Exception {
		Response resp = link.request("hincr", name, key,
				(new Long(by)).toString());
		if (!resp.ok()) {
			resp.exception();
		}
		if (resp.raw.size() != 2) {
			throw new Exception("Invalid response");
		}
		long ret = 0;
		ret = Long.parseLong(new String(resp.raw.get(1)));
		return ret;
	}

	/* zset */

	private boolean zset(String name, byte[] key, long score) throws Exception {
		Response resp = link.request("zset", name.getBytes(AbstractRedisDao.DEFAULT_ENCODING), key, (new Long(
				score)).toString().getBytes(AbstractRedisDao.DEFAULT_ENCODING));
		if (!resp.ok()) {
			resp.exception();
		}
		return true;
	}

	public boolean zset(String name, String key, long score) throws Exception {
		return zset(name, key.getBytes(AbstractRedisDao.DEFAULT_ENCODING), score);
	}

	private boolean zdel(String name, byte[] key) throws Exception {
		Response resp = link.request("zdel", name.getBytes(AbstractRedisDao.DEFAULT_ENCODING), key);
		if (!resp.ok()) {
			resp.exception();
		}
		return true;
	}

	public boolean zdel(String name, String key) throws Exception {
		return zdel(name, key.getBytes(AbstractRedisDao.DEFAULT_ENCODING));
	}
	
//	public long zcount(String name) throws Exception {
//		return zcount(name, Constants.EMPTY_STRING, Constants.EMPTY_STRING);
//	}
	
	public long zcount(String name, String start, String end) throws Exception {
		Response resp = link.request("zcount", name.getBytes(AbstractRedisDao.DEFAULT_ENCODING), start.getBytes(AbstractRedisDao.DEFAULT_ENCODING),
				end.getBytes(AbstractRedisDao.DEFAULT_ENCODING));
		if (!resp.ok()) {
			resp.exception();
		}
		return Long.parseLong(new String(resp.raw.get(1)));
	}

	public boolean zclear(String name) throws Exception {
		Response resp = link.request("zclear", name.getBytes(AbstractRedisDao.DEFAULT_ENCODING));
		if (!resp.ok()) {
			resp.exception();
		}
		return true;
	}
	
	/**
	 * 
	 * @param name
	 * @param key
	 * @return null if not found.
	 * @throws Exception
	 */
	private Long zget(String name, byte[] key) throws Exception {
		Response resp = link.request("zget", name.getBytes(AbstractRedisDao.DEFAULT_ENCODING), key);
		if (resp.not_found()) {
			return null;
		}
		if (resp.raw.size() != 2) {
			throw new Exception("Invalid response");
		}
		if (!resp.ok()) {
			resp.exception();			
		}
		return Long.valueOf(new String(resp.raw.get(1)));
	}

	/**
	 * 
	 * @param name
	 * @param key
	 * @return null if not found.
	 * @throws Exception
	 */
	public Long zget(String name, String key) throws Exception {
		return zget(name, key.getBytes(AbstractRedisDao.DEFAULT_ENCODING));
	}

	private Response _zscan(String cmd, String name, String key,
			Long score_start, Long score_end, int limit) throws Exception {
		if (key == null) {
			key = "";
		}
		String ss = "";
		if (score_start != null) {
			ss = score_start.toString();
		}
		String se = "";
		if (score_start != null) {
			se = score_end.toString();
		}
		Response resp = link.request(cmd, name, key, ss, se,
				(new Integer(limit)).toString());
		if (!resp.ok()) {
			resp.exception();
		}
		for (int i = 1; i < resp.raw.size(); i += 2) {
			byte[] k = resp.raw.get(i);
			byte[] v = resp.raw.get(i + 1);
			resp.keys.add(k);
			resp.items.put(k, v);
		}
		return resp;
	}

	private Response zscan(String name, String key, Long score_start,
			Long score_end, int limit) throws Exception {
		return this._zscan("zscan", name, key, score_start, score_end, limit);
	}

	private Response zrscan(String name, String key, Long score_start,
			Long score_end, int limit) throws Exception {
		return this._zscan("zrscan", name, key, score_start, score_end, limit);
	}

	public long zincr(String name, String key, long by) throws Exception {
		return  zincr(name,  key.getBytes(AbstractRedisDao.DEFAULT_ENCODING), by);
	}
	
	public long zincr(String name, byte[] key, long by) throws Exception {
		Response resp = link.request("zincr",  name.getBytes(AbstractRedisDao.DEFAULT_ENCODING), key, 
				String.valueOf(by).getBytes(AbstractRedisDao.DEFAULT_ENCODING));
		if (!resp.ok()) {
			resp.exception();
		}
		if (resp.raw.size() != 2) {
			throw new Exception("Invalid response");
		}		
		return Long.parseLong(new String(resp.raw.get(1)));
	}

	public long zdecr(String name, String key, long by) throws Exception {
		return zdecr(name, key.getBytes(AbstractRedisDao.DEFAULT_ENCODING), by);
	}
	
	private long zdecr(String name, byte[] key, long by) throws Exception {
		Response resp = link.request("zdecr", name.getBytes(AbstractRedisDao.DEFAULT_ENCODING), key, 
				String.valueOf(by).getBytes(AbstractRedisDao.DEFAULT_ENCODING));
		if (!resp.ok()) {
			resp.exception();
		}
		if (resp.raw.size() != 2) {
			throw new Exception("Invalid response");
		}
		return Long.parseLong(new String(resp.raw.get(1)));
	}
	
	/****************/
	public Map<byte[],byte[]> multiHget(String hashName,String... keys) throws Exception {
		 String newKeys [] = new String[keys.length+1];
	     newKeys[0] = hashName;
	     
	     System.arraycopy(keys, 0, newKeys, 1, keys.length);
		Response resp = link.request("multi_hget",newKeys);
		if (!resp.ok()) {
			resp.exception();
		}
		resp.buildMap();
		return resp.items;
	}
	

	public Map<byte[],byte[]>  multiGet(String... keys) throws Exception {
		Response resp = link.request("multi_get", keys);
		if (!resp.ok()) {
			resp.exception();
		}
		resp.buildMap();
		return resp.items;
	}

	public Map<byte[],byte[]> multiGet(byte[]... keys) throws Exception {
		Response resp = link.request("multi_get", keys);
		if (!resp.ok()) {
			resp.exception();
		}
		resp.buildMap();
		return resp.items;
	}

	public boolean multiSet(String... kvs) throws Exception {
		if (kvs.length % 2 != 0) {
			throw new Exception("Invalid arguments count");
		}
		Response resp = link.request("multi_set", kvs);
		if (!resp.ok()) {
			resp.exception();
		}
		return true;
	}

	public boolean multiSet(byte[]... kvs) throws Exception {
		if (kvs.length % 2 != 0) {
			throw new Exception("Invalid arguments count");
		}
		Response resp = link.request("multi_set", kvs);
		if (!resp.ok()) {
			resp.exception();
		}
		return true;
	}
	
	public boolean multiHset( byte[]... kvs) throws Exception {
		
		Response resp = link.request("multi_hset" ,kvs);
		if (!resp.ok()) {
			resp.exception();
		}
		return true;
	}

	public boolean multiDel(String... keys) throws Exception {
		Response resp = link.request("multi_del", keys);
		if (!resp.ok()) {
			resp.exception();
		}
		
		return true;
	}
	
	public boolean multiHdel(String hashName, String... keys) throws Exception {
		String newKeys [] = new String[keys.length+1];
	     newKeys[0] = hashName;
	     
	     System.arraycopy(keys, 0, newKeys, 1, keys.length);
		Response resp = link.request("multi_hdel",newKeys);
		
		if (!resp.ok()) {
			resp.exception();
		}
		
		return true;
	}

	public boolean multiDel(byte[]... keys) throws Exception {
		Response resp = link.request("multi_del", keys);
		if (!resp.ok()) {
			resp.exception();
		}
		return true;
	}

	public SSDBPool getPool() {
		return pool;
	}

	public void setPool(SSDBPool pool) {
		this.pool = pool;
	}
	
	
}
