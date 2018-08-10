package com.dhgate.memcache.test;

import static org.junit.Assert.assertTrue;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import com.dhgate.XMLConfiguration;
import com.dhgate.redis.IRedisDao;
import com.dhgate.redis.RedisLock;

public class RedisLockTest {

	IRedisDao cache ;
	String lockKey="test_redis_lock";
	
	@Before
	public void start() {
		 cache = XMLConfiguration.getInstance().getRedisCacheCluster("redisLockCluster");
	}
	
	@Test
	public void testLock () {
		cache.delKey(lockKey);
		
		RedisLock lock = cache.getLock(lockKey,10,TimeUnit.SECONDS);
		lock.lock();
		try {
			System.out.println("the thread : " + Thread.currentThread().getId() +" got lock success");
			String v = cache.getHashString(lockKey, lock.getLockName(Thread.currentThread().getId()));
			Long r = Long.valueOf(v);
			assertTrue(r > 0 && r < 10*1000);
		}finally {
			lock.unlock();
		}
	}
	
	@Test
	public void testUnLock () throws InterruptedException {
		cache.delKey(lockKey);
		RedisLock lock = cache.getLock(lockKey,10,TimeUnit.SECONDS);
		lock.lock();
		try {
			System.out.println("the thread : " + Thread.currentThread().getId() +" got lock success");
			//String v = cache.getHashString(lockKey, lock.getLockName(Thread.currentThread().getId()));
		}finally {
			lock.unlock();
		}
		System.out.println("the thread : " + Thread.currentThread().getId() +" unLock success");
		
		String v = cache.getHashString(lockKey, lock.getLockName(Thread.currentThread().getId()));
		
		assertTrue(v==null);
	}
	
	@Test
	public void testLockExclusion () throws InterruptedException {
		
		cache.delKey(lockKey);
		final RedisLock lock = cache.getLock(lockKey,5,TimeUnit.MINUTES);
		ExecutorService es = Executors.newSingleThreadExecutor();
		 Runnable run = new Runnable() {
			
			@Override
			public void run() {
				lock.lock();
				try {
					System.out.println("the thread : " + Thread.currentThread().getId() +" got lock success");
					String v = cache.getHashString(lockKey, lock.getLockName(Thread.currentThread().getId()));					
					try {
						Thread.sleep(50000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				} finally {
					System.out.println("the thread : " + Thread.currentThread().getId() +" unLock success");
					lock.unlock();
				}
			}
		};
		es.execute(run);
		
		Thread.sleep(100); //waiting the Thread#th to got lock;
		System.out.println("to get lock again......");
		if (lock.tryLock(1,TimeUnit.SECONDS)) {
			try {
				
				System.out.println("the thread : " + Thread.currentThread().getId() +" got lock success");
				assertTrue(false);
			}finally {
				lock.unlock();
			}
		} else {
			assertTrue(true);
		}
		
		es.shutdown();
	}
	
	
	
}
