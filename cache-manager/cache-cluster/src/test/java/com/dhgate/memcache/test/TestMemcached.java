package com.dhgate.memcache.test;

import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import com.dhgate.XMLConfiguration;
import com.dhgate.memcache.IMemCached;
import com.dhgate.redis.IRedisDao;

public class TestMemcached {
	private static Random rad = new Random(120);
	public static void main (String args []) throws InterruptedException {
		//IMemCached cache = DistributionCacheManager.getInstance().getCacheCluster("buyerSession");
		IRedisDao dao = XMLConfiguration.getInstance().getRedisCacheCluster("lockCluster");
		String lockKey = "testLdkLock";
		Lock lock = dao.getLock(lockKey,2,TimeUnit.SECONDS );
		for (int i=0;i<10;i++)
			testRedisLock(lock,i).start();
		System.out.println("=========== end");
		
		//ISSDBDao dao = DistributionCacheManager.getInstance().getSSDBCluster(clusterName);
		
	  /* int r=24*60;
		String key="testexpire";
		cache.set(key, "ffffdddddfff",r);
		System.out.println(cache.get(key));
		cache.set(key, false);
		System.out.println(cache.get(key));
		
		cache.set(key, 1000);
		System.out.println(cache.get(key));
		cache.set(key, 1356.15);
		System.out.println(cache.get(key));
		
		
		
		cache.set(key, true);
		System.out.println(cache.get(key));
		
		List <String>lis = new ArrayList<String>();
		lis.add("fsssssssssssss");
		lis.add("abb");
	    lis.add("eeee");
		cache.set("test2", lis);
		System.out.println(key);*/
		
		//cache.delete("test2");
		
	//	IMemCached cache2 = MemcachedCacheManager.getInstance().getCacheCluster("cluster1");
		//cache2.set("qazTest", "qazTest");
		
		/*while (true) {
			try {
				//cache.set("test", "yyyy==" + cn);
				System.out.println(cache.get(key) +"  =======11");
				//System.out.println(cache.get("test2") +"  ======22");
				//System.out.println(cache2.get("qazTest") +"  ======333333");
			
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			Thread.sleep(10);
			
		}*/
		
		
		/*System.out.println(cache.get("test") +"  =======");
		cache.set("test2", "yyyy");
		cache.set("test3", "yyyy");
		cache.set("test4", "yyyy");
		int cn =0;
		while (true) {
			//try {
				cache.set("test", "yyyy==" + cn);
				System.out.println(cache.get("test") +"  =======");
			
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			Thread.sleep(10);
			cn ++;
		}*/
		
		//ISSDBDao dao = DistributionCacheManager.getInstance().getSSDBCluster(name)
/*
		IRedisDao cache = DistributionCacheManager.getInstance().getRedisCacheCluster("cluster1");
		
		
		int r=24;
		String key ="test";
		
		cache.setString(key, "ffffffffff",0);
		System.out.println(cache.getString(key) +"  =======");
		int cn =0;
		
		while (true) {
			try {
			//cache.set(key, "sss"+cn);
			System.out.println(cache.getString(key) +"  =======");
			} catch (Exception e) {
				e.printStackTrace();
			}
			Thread.sleep(10);
			cn++;
		}
		*/
		//MemcachedCacheManager.getInstance().getCache(name)
		//Boolean b = true;
		//ThreadPoolExecutor tp = new ThreadPoolExecutor(30, 50, 10, TimeUnit.MINUTES,new LinkedBlockingQueue<Runnable>(1000));
		//cache.delete("60dc88df704962ffa11e58cd392c3cbb");
		//cache.set(key, value, expiry)
		
		//System.out.println(cache.get("60dc88df704962ffa11e58cd392c3cbb") +" r========================");
		
		//IMemCached buyer = MemcachedCacheManager.getInstance().getCache("buyer");
		//System.out.println(cacheSeller.get("DHmobile_user_25258534-d251-483b-ba54-eac588754641"));
	  //  java.lang.reflect.Proxy.newProxyInstance(loader, interfaces, h)
	}
	
	public static Thread testRedisLock (final Lock lock,final int n) {
		Runnable r = new Runnable() {
			
			@Override
			public void run() {
				lock.lock();
				try{
				System.out.println(n + "=======Thread="+Thread.currentThread().getId() +"  got lock sueess");
				Thread.sleep(1000);
				} catch (Exception e) {
					e.printStackTrace();
				}finally {
					lock.unlock();
					System.out.println(n + "=======Thread="+Thread.currentThread().getId() +"   unlock finished");
				}
				
			}
		};
		
		return new Thread(r);
	}
	
	
	public static void testCase () {
		BlockingQueue<String> qu = new LinkedBlockingQueue<String>(10000);
		IMemCached cache = XMLConfiguration.getInstance().getCacheCluster("cluster1");
		System.out.println(cache.get("test"));
		for (int i=0;i<2;i++) {
			//String key="ldk_test_"+rad.nextInt();
			//boolean isGet=(i%2==0)?true:false;
			TestTask tsk = new TestTask (cache,qu,false);
			Thread th = new Thread(tsk);
			th.start();
		}
		
		System.out.println(cache.get("end c thread"));
		
		while (true) {
			String key="ldk_test_"+rad.nextInt();
			try {
				qu.put(key);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}
	
	public static class TestTask implements Runnable {

		IMemCached cache;
		BlockingQueue<String> key;
		boolean isGet;
		public TestTask (IMemCached cache,BlockingQueue<String> key,boolean isGet){
			this.cache = cache;
			this.key = key;
			this.isGet = isGet;
		}
		@Override
		public void run() {
			try {
			while (true) {
				String tkey = key.take();
				//if (isGet) {
				cache.set(tkey, rad.nextFloat(),10);
				System.out.println("to get++++========="+ cache.get(tkey));
			    cache.delete(tkey);
			    tkey=null;
				
			}
			} catch (Exception e ){
				e.printStackTrace();
			}
			
		}
		
		
	}
	
}
