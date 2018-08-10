package com.dhgate.redis;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base Jedis client
 * Distributed implementation of {@link java.util.concurrent.locks.Lock}
 * Implements reentrant lock.<br>
 * Lock will be removed automatically if client disconnects.
 * Implements a <b>non-fair</b> locking so doesn't guarantees an acquire order.
 * 
 * @author lidingkun
 *
 */

public class RedisLock implements Lock{
	private static final Logger log = LoggerFactory.getLogger(RedisLock.class);
	private final static String PUB_CHANNEL = "redis_lock__channel"; 
	
	private  boolean fair = false;
	private final UUID id;
	private String lockKey;
	private IRedisDao dao;
	private Lock lock = new ReentrantLock();
	private Condition condition = lock.newCondition();
	private Long leaseTime = 1000L;  //default 1Second
	public RedisLock(UUID id){
		this.id = id;
	}
	
	public RedisLock(boolean fair,UUID id,String lockKey,IRedisDao dao) {
		this.fair = fair;
		this.id = id;
		this.lockKey = lockKey;
		this.dao = dao;
	}
	
	@Override
	public void lock() {
		try {
			lockInterruptibly();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}
	@Override
	public void lockInterruptibly() throws InterruptedException {
		long threadId = Thread.currentThread().getId();
        Long ttl = tryAcquire(threadId);
        if (ttl == null) {
            return;
        }
        RedisLockSub sub = this.subscribe(threadId);

        try {
            while (true) {
	                ttl = tryAcquire(threadId);
	                // lock acquired
	                if (ttl == null) {
	                    break;
	                }
	                
	                if (ttl >= 0) {
	                	lock.lock();
	                	 try {
	                		 this.condition.await(ttl,TimeUnit.MILLISECONDS);
	                	 } finally {
	     	            	lock.unlock();
	     	            }
	                 }
	                } 
	            
        } finally {
            unsubscribe(sub);
        }
	}

	@Override
	public boolean tryLock() {
		try {
			return tryLock(-1,null);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		return false;
	}

	@Override
	public boolean tryLock(long waitTime, TimeUnit unit) throws InterruptedException {
		long threadId = Thread.currentThread().getId();
        Long ttl = tryAcquire(threadId);
        // lock acquired
        if (ttl == null) {
           return true;
        }
		if (waitTime > 0) {
			waitTime = unit.toNanos(waitTime);
			long s = System.nanoTime();
			RedisLockSub sub = this.subscribe(threadId);
			try {
				while (true) {
					ttl = tryAcquire(threadId);
					// lock acquired
					if (ttl == null) {
						return true;
					}
				
					if (waitTime >= (System.nanoTime()-s)) {
						lock.lock();
						try {
							this.condition.await(waitTime/2, TimeUnit.NANOSECONDS);
						} finally {
							lock.unlock();
						}
					} else {
						break;
					}
				}
			} finally {
				unsubscribe(sub);
			}
		}

		return false;
	}
	

	@Override
	public Condition newCondition() {
		// TODO Auto-generated method stub
		 throw new UnsupportedOperationException();
	}

	
	public Long getLeaseTime() {
		return leaseTime;
	}

	public void setLeaseTime(Long leaseTime) {
		this.leaseTime = leaseTime;
	}

	public String getLockKey() {
		return lockKey;
	}

	public void setLockKey(String lockKey) {
		this.lockKey = lockKey;
	}

	public IRedisDao getDao() {
		return dao;
	}

	public void setDao(IRedisDao dao) {
		this.dao = dao;
	}

	public boolean isFair() {
		return fair;
	}

	public void setFair(boolean fair) {
		this.fair = fair;
	}
	@Override
	public void unlock() {
		String script =  "if (redis.call('exists', KEYS[1]) == 0) then " + "redis.call('publish', KEYS[2], ARGV[1]); return 1; " +
             "end; if (redis.call('hexists', KEYS[1], ARGV[3]) == 0) then return nil; end; " +
             "local counter = redis.call('hincrby', KEYS[1], ARGV[3], -1); " +
             "if (counter > 0) then " +
                 "redis.call('pexpire', KEYS[1], ARGV[2]); " +
                 "return 0; " +
             "else " +
                 "redis.call('del', KEYS[1]); " +
                 "redis.call('publish', KEYS[2], ARGV[1]); " +
                 "return 1; "+
             "end; " +
             "return nil;";
		
		List<String> keys = new ArrayList<String> ();
		keys.add(this.lockKey);
		keys.add(this.getChannelName());
		List<String> args = new ArrayList<String> ();
		args.add(RedisLockSub.UNLOCK_MESSAGE);
		args.add(String.valueOf(this.leaseTime.longValue()));
		args.add(this.getLockName(Thread.currentThread().getId()));	
		int n = 0;
		try {
			Object o = dao.eval(script, keys, args);
			if (o == null) {
				log.error("IllegalMonitorStateException keys= "+ keys + " args ="+args );
			} else {
				n=Integer.parseInt(o.toString());
			}
		} catch (Exception e) {
			log.error("unlock" ,e);
		}
		
		if (n == 1) {
			lock.lock();
			try {
			this.condition.signalAll();
			}finally {
				lock.unlock();
			}
		}
	}
	
	private Long tryAcquire(Long threadId) throws InterruptedException {
		String script = "if (redis.call('exists', KEYS[1]) == 0) then redis.call('hset', KEYS[1], ARGV[2], 1); redis.call ('pexpire',KEYS[1],ARGV[1]); return nil; end; if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then redis.call('hincrby', KEYS[1], ARGV[2], 1);redis.call('pexpire', KEYS[1], ARGV[1]); return nil; end; return redis.call('pttl', KEYS[1]);";
		List<String> keys = new ArrayList<String> ();
		keys.add(this.lockKey);
		List<String> args = new ArrayList<String> ();
		args.add(String.valueOf(this.leaseTime.longValue()));
		args.add(this.getLockName(threadId));
		Long r =  -1L;
		try {
			Object o = this.dao.eval(script, keys, args);
			if ( o == null) {
				return null;
			}
			r =Long.valueOf(o.toString());
		} catch (Exception e) {
			log.error("tryAcquire : " , e);
		}
		return r;
	}

	private void unsubscribe(RedisLockSub sub) {
       sub.unsubscribe();
    }
	
	
	private RedisLockSub subscribe(long threadId) {
		 RedisLockSub sub = new RedisLockSub (this.getChannelName(),this.condition,this.lock);
		 this.dao.subscribe(sub, this.getChannelName());
		 return sub;
	}

	 
	private String getChannelName() {
		return PUB_CHANNEL + ":" + this.lockKey;
	}

	public String getLockName(long threadId) {
		return id + ":" + threadId;
	}
}
