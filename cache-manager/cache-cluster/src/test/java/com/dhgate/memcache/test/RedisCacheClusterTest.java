package com.dhgate.memcache.test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import com.dhgate.XMLConfiguration;
import com.dhgate.redis.IRedisDao;

public class RedisCacheClusterTest {

	IRedisDao cache ;
	String key="addTest";
	String value ="testRedsCase";
	
	@Before
	public void start() {
		 cache = XMLConfiguration.getInstance().getRedisCacheCluster("redisLockCluster");
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
		cache.delKey(key);
		v = cache.getString(key);
		assertFalse(v != null);
	}

	
	@Test
	public void testTTL () {
		cache.delKey(key);
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
}
