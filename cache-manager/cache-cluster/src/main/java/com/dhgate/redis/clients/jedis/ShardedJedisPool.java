package com.dhgate.redis.clients.jedis;

import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;

import com.dhgate.Hashing;
import com.dhgate.redis.clients.util.Pool;

public class ShardedJedisPool extends Pool<ShardedJedis> {
    public ShardedJedisPool(final GenericObjectPool.Config poolConfig,List<JedisShardInfo> shards,boolean isPolling) {
	this(poolConfig, shards, Hashing.MURMUR_HASH, isPolling);
    }

    public ShardedJedisPool(final GenericObjectPool.Config poolConfig,
	    List<JedisShardInfo> shards, Hashing algo,boolean isPolling) {
	this(poolConfig, shards, algo, null, isPolling);
    }

    public ShardedJedisPool(final GenericObjectPool.Config poolConfig,List<JedisShardInfo> shards, Pattern keyTagPattern,boolean isPolling) {
	this(poolConfig, shards, Hashing.MURMUR_HASH, keyTagPattern, isPolling);
    }

    public ShardedJedisPool(final GenericObjectPool.Config poolConfig,
	    List<JedisShardInfo> shards, Hashing algo, Pattern keyTagPattern,boolean isPolling) {
	super(poolConfig, new ShardedJedisFactory(shards, algo, keyTagPattern, isPolling));
    }

    @Override
    public ShardedJedis getResource() {
	ShardedJedis jedis = super.getResource();
	jedis.setDataSource(this);
	return jedis;
    }

    @Override
    public void returnBrokenResource(final ShardedJedis resource) {
	if (resource != null) {
	    returnBrokenResourceObject(resource);
	}
    }

    @Override
    public void returnResource(final ShardedJedis resource) {
	if (resource != null) {
	    resource.resetState();
	    returnResourceObject(resource);
	}
    }

    /**
     * PoolableObjectFactory custom impl.
     */
    private static class ShardedJedisFactory extends BasePoolableObjectFactory<ShardedJedis> {
	private List<JedisShardInfo> shards;
	private Hashing algo;
	private Pattern keyTagPattern;
	private boolean isPolling;
	public ShardedJedisFactory(List<JedisShardInfo> shards, Hashing algo,Pattern keyTagPattern,boolean isPolling) {
	    this.shards = shards;
	    this.algo = algo;
	    this.keyTagPattern = keyTagPattern;
	    this.isPolling = isPolling;
	}

	@Override
	public ShardedJedis makeObject() throws Exception {
	    ShardedJedis jedis = new ShardedJedis(shards, algo, keyTagPattern, isPolling);
	    return jedis;
	}

	public void destroyObject(ShardedJedis shardedJedis)
		throws Exception {
	    for (Jedis jedis : shardedJedis.getAllShards()) {
		try {
		    try {
			jedis.quit();
		    } catch (Exception e) {

		    }
		    jedis.disconnect();
		} catch (Exception e) {

		}
	    }
	}

	public boolean validateObject(
		ShardedJedis jedis) {
	    try {
		for (Jedis shard : jedis.getAllShards()) {
		    if (!shard.ping().equals("PONG")) {
			return false;
		    }
		}
		return true;
	    } catch (Exception ex) {
		return false;
	    }
	}

	public void activateObject(ShardedJedis p)
		throws Exception {

	}

	public void passivateObject(ShardedJedis p)
		throws Exception {

	}
    }
}