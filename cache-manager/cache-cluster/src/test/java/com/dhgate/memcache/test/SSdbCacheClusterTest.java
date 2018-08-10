package com.dhgate.memcache.test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.dhgate.XMLConfiguration;
import com.dhgate.ssdb.ISSDBDao;

public class SSdbCacheClusterTest {

	ISSDBDao cache ;
	String key="addTest";
	String value ="testSSDBCase";
	
	@Before
	public void start() {
		 cache = XMLConfiguration.getInstance().getSSDBCluster ("ssdbCluster");
	}
	
	@Test
	public void testSet () {
		cache.setString(key,value , 5);
		String v = cache.getString(key);
		assertTrue(value.equals(v));
	}

	@Test
	public void testDelete () {
		cache.setString(key,value , 5);
		String v = cache.getString(key);
		if (v == null || ! value.equals(v)) {
			throw new RuntimeException(" got value is error for v = " + v);
		}
		cache.del(key);
		v = cache.getString(key);
		assertFalse(v != null);
	}

	
	@Test
	public void testTTL () {
		cache.del(key);
		cache.setString(key,value , 2);
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String v = cache.getString(key);
		if (v == null || ! value.equals(v)) {
			throw new RuntimeException(" got value is error for v = " + v);
		}
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 v = cache.getString(key);
		 System.out.println("v == " + v);
		assertFalse(v != null);
	}
	
	
	@Test
	public void testHset () {
		String hashName ="tHashSet";
		String hKey="tHashSet_key1";
		
		
		cache.hsetObject(hashName, hKey, value);
		String v = cache.hgetString(hashName, hKey);
		assertTrue(value.equals(v));
		
	}
	
	@Test
	public void testHgetALL  () {
		String hashName ="tHashSet";
		String hKey="tHashSet_key1";
		
		cache.hclear(hashName);
		cache.hsetObject(hashName, hKey, value);
		cache.hsetObject(hashName, hKey+"_1", value+1);
		cache.hsetObject(hashName, hKey+"_2", value+2);
		Map<String,Object> r = cache.hgetall(hashName);
		
		assertTrue(r.size() == 3 && r.get(hKey).equals(value));
		
	}
	
	@Test
	public void testHmultiSet  () {
		String hashName ="tHashSet";
		String hKey="tHashSet_key1";
		Map<String,Object> map = new HashMap<String, Object>();
		cache.hclear(hashName);
		map.put( hKey, value);
		map.put( hKey+"_1", value+1);
		map.put(hKey+"_2", value+2);
		map.put(hKey+"_3", value+3);
		cache.multiHset(hashName, map);
		Map<String,Object> r = cache.hgetall(hashName);
		
		assertTrue(r.size() == 4 && r.get(hKey+"_3").equals(value+3));
	}
	
	@Test
	public void testHmultiDel  () {
		String hashName ="tHashSet";
		String hKey="tHashSet_key1";
		Map<String,Object> map = new HashMap<String, Object>();
		cache.hclear(hashName);
		map.put( hKey, value);
		map.put( hKey+"_1", value+1);
		map.put(hKey+"_2", value+2);
		map.put(hKey+"_3", value+3);
		String keys[] = new String[]{hKey,hKey+"_1",hKey+"_2"};
		cache.multiHset(hashName, map);
		System.out.println(cache.hgetall(hashName).keySet());
		try {
			Thread.sleep(100);
			cache.multiHdel(hashName,keys);
			Thread.sleep(100);
		}catch (Exception e) {
			e.printStackTrace();
		}
		Map<String,Object> r = cache.hgetall(hashName);
		System.out.println(r.values());
		assertTrue(r.size() == 1);
		
	}
	
	@Test
	public void testMultiDel  () {
		
		String hKey="tDeltest_key1";
		Map<String,Object> map = new HashMap<String, Object>();
	
		map.put( hKey, value);
		map.put( hKey+"_1", value+1);
		map.put(hKey+"_2", value+2);
		map.put(hKey+"_3", value+3);
		cache.multiSet(map);
		try {
		Thread.sleep(100);
		cache.multiDel(new String[]{hKey,hKey+"_1",hKey+"_2"});
		Thread.sleep(100);
		}catch (Exception e) {
			e.printStackTrace();
		}
		
		List <Object> r = cache.multiGet(map.keySet().toArray(new String[0]));
		
		
		assertTrue(r.size() == 1 && r.get(0).equals(value+3));
	}
	
	@Test
	public void testHashMultiDel  () {
		String hashName ="tHashSet";
		String hKey="tHashSet_key1";
		Map<String,Object> map = new HashMap<String, Object>();
		cache.hclear(hashName);
		map.put( hKey, value);
		map.put( hKey+"_1", value+1);
		map.put(hKey+"_2", value+2);
		map.put(hKey+"_3", value+3);
		String keys[] = new String[]{hKey,hKey+"_1",hKey+"_2"};
		cache.multiHset(hashName, map);
		System.out.println(cache.hgetall(hashName).keySet());
		try {
			Thread.sleep(100);
			cache.multiHdel(hashName,keys);
			Thread.sleep(100);
		}catch (Exception e) {
			e.printStackTrace();
		}
		Map<String,Object> r = cache.hgetall(hashName);
		System.out.println(r.values());
		assertTrue(r.size() == 1);
		
	}
	
	
	@Test
	public void testHkeys  () {
		String hashName ="tHashSet";
		String hKey="tHashSet_key";
		String keyStart = hKey;
		String keyEnd = hKey+"_3";
		cache.hclear(hashName);
		Map<String,Object> map = new HashMap<String, Object>();
		
		String k1=hKey+"_0";
		String v1=value+"_0";
		String k2=hKey+"_1";
		String v2=value+"_1";
		String k3=hKey+"_2";
		String v3=value+"_2";
		
		map.put(k1, v1);
		map.put(k2, v2);
		map.put(k3, v3);
		cache.multiHset(hashName, map);
		List<String> r = cache.hkeys(hashName, keyStart, keyEnd, 3);
		System.out.println("testHkeys: output"  +  r);
		String k = r.get(0);
		String v =(String) cache.hgetObject(hashName, k); 
		System.out.println("k v: output  "  +  k +" : " + v);
		boolean kv =((k.equals(k1) && v.equals(v1)) || (k.equals(k3) && v.equals(v3)) || (k.equals(k2) && v.equals(v2)) )?true : false;
		assertTrue(r.size() == 3 && kv);
		
	}
	
	@Test
	public void testkeys  () {
		String key="ttest_key";
		String value="testkeys_value";
		String keyStart = key;
		String keyEnd = key+"_3";
		Map<String,Object> map = new HashMap<String, Object>();
		String k1=key+"_0";
		String v1=value+"_0";
		String k2=key+"_1";
		String v2=value+"_1";
		String k3=key+"_2";
		String v3=value+"_2";
		
		map.put(k1, v1);
		map.put(k2, v2);
		map.put(k3, v3);
		cache.multiSet(map);
		
		List<String> r = cache.keys(keyStart, keyEnd, 3);
		System.out.println("testkeys: output"  +  r);
		String k = r.get(0);
		String v =(String) cache.getObject(k); 
		System.out.println("k v: output "  +  k +" : " + v);
		boolean kv =((k.equals(k1) && v.equals(v1)) || (k.equals(k3) && v.equals(v3)) || (k.equals(k2) && v.equals(v2)) )?true : false;
		
		
		
		assertTrue(r.size() == 3 && kv);
		
	}
	
}
