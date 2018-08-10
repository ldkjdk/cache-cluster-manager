package com.dhgate.memcache.test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import com.dhgate.cache.config.diamond.DiamondConfiguration;
import com.dhgate.memcache.IMemCached;

public class DiamondConfigurationTest {
	IMemCached cache ;
	String key="test2";
	String addV="ffffff";
	String replaceV="dddddd";
	@Before
	public void start() {
		 cache = DiamondConfiguration.getInstance("test_memcached", "com.dhgate.dtb.cache").getCacheCluster("cluster1");
	}
	
	@Test
	public void testAdd () {
		cache.delete(key);
		boolean f = cache.add(key, addV);
		String v = cache.get(key).toString();
		assertTrue(addV.equals(v) && f);
	}

	@Test
	public void testAdd2 () {
		cache.delete(key);
		 cache.add(key, addV);
		 boolean f = cache.add(key, replaceV);
		String v = cache.get(key).toString();
		assertTrue(addV.equals(v) && f == false);
	}
	
	@Test
	public void testReplace1 () {
		cache.delete(key);
		//cache.add(key, addV);
		boolean f = cache.replace(key, "dddd");
		assertFalse(f);
	}
	
	@Test
	public void testReplace2 () {
		cache.delete(key);
		cache.add(key, addV);
		boolean f = cache.replace(key, replaceV);
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String v = cache.get(key).toString();
		assertTrue(replaceV.equals(v) && f);
	}
}
