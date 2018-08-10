package com.dhgate.redis;


import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import com.dhgate.redis.clients.jedis.JedisPubSub;

/**
 * 
 * @author lidingkun
 *
 */

public class RedisLockSub extends JedisPubSub{
	public static final String UNLOCK_MESSAGE = "ulok";
	private String channels = null;
	private Condition condition;
	private Lock lock;
	
	public RedisLockSub(String channel, Condition condition, Lock lock) {
		super();
		this.channels = channel;
		this.condition = condition;
		this.lock = lock;
	}

	@Override
	public void onMessage(String channel, String message) {
		super.onMessage(channel, message);
		
		if (this.channels.equals(channel) && UNLOCK_MESSAGE.equals(message)) {
			lock.lock();
			try {
				this.condition.signalAll();
			} finally {
				lock.unlock();
			}
		}
	}

	
}
