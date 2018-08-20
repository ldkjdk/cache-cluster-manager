package com.dhgate.memcache.test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import com.dhgate.XMLConfiguration;
import com.dhgate.memcache.IMemCached;

public class MemcachedCacheClusterTest {
	IMemCached cache ;
	String key="test2";
	String addV="ffffff";
	String replaceV="dddddd";
	@Before
	public void start() {
		
		 cache = XMLConfiguration.getInstance().getCacheCluster("memcachedCluster");
	}
	
	@Test
	public void testAdd () {
		cache.delete(key);
		boolean f = cache.add(key, addV);
		String v = cache.get(key).toString();
		assertTrue(addV.equals(v) && f);
	}

	@Test
	public void testAdd2 () throws InterruptedException {
		cache.delete(key);
		 cache.add(key, addV);
		 Thread.sleep(100);
		 boolean f = cache.add(key, replaceV);
		String v = cache.get(key).toString();
		assertTrue(addV.equals(v) && f == false);
	}
	
	@Test
	public void testReplace1 () throws Exception {
		cache.delete(key);
		Thread.sleep(100);
		//cache.add(key, addV);
		boolean f = cache.replace(key, "dddd");
		assertFalse(f);
	}
	
	@Test
	public void testReplace2 () throws InterruptedException {
		cache.delete(key);
		cache.add(key, addV);
		boolean f = cache.replace(key, replaceV);
		Thread.sleep(100); // waiting all node execute replace finished of the cluster;
		String v = cache.get(key).toString();
		assertTrue(replaceV.equals(v) && f);
	}
}
