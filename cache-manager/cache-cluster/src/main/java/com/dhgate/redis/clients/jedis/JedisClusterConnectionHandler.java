package com.dhgate.redis.clients.jedis;

import static com.dhgate.redis.clients.jedis.JedisClusterInfoCache.getNodeKey;

import java.util.Map;
import java.util.Set;

import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.commons.pool.impl.GenericObjectPool.Config;

import com.dhgate.redis.clients.jedis.exceptions.JedisConnectionException;

public abstract class JedisClusterConnectionHandler {
    protected final JedisClusterInfoCache cache;

    abstract Jedis getConnection();

    public void returnConnection(Jedis connection) {
	cache.getNode(getNodeKey(connection.getClient())).returnResource(
		connection);
    }

    public void returnBrokenConnection(Jedis connection) {
	cache.getNode(getNodeKey(connection.getClient())).returnBrokenResource(
		connection);
    }

    abstract Jedis getConnectionFromSlot(int slot);

    public Jedis getConnectionFromNode(HostAndPort node) {
	cache.setNodeIfNotExist(node);
	return cache.getNode(JedisClusterInfoCache.getNodeKey(node))
		.getResource();
    }

    public JedisClusterConnectionHandler(Set<HostAndPort> nodes,
	    final GenericObjectPool.Config poolConfig) {
	this.cache = new JedisClusterInfoCache(poolConfig);
	initializeSlotsCache(nodes, poolConfig);
    }

    public Map<String, JedisPool> getNodes() {
	return cache.getNodes();
    }

    public void assignSlotToNode(int slot, HostAndPort targetNode) {
	cache.assignSlotToNode(slot, targetNode);
    }

    private void initializeSlotsCache(Set<HostAndPort> startNodes,
    		GenericObjectPool.Config poolConfig) {
	for (HostAndPort hostAndPort : startNodes) {
	    JedisPool jp = new JedisPool(poolConfig, hostAndPort.getHost(),
		    hostAndPort.getPort());

	    Jedis jedis = null;
	    try {
		jedis = jp.getResource();
		cache.discoverClusterNodesAndSlots(jedis);
		break;
	    } catch (JedisConnectionException e) {
		// try next nodes
	    } finally {
		if (jedis != null) {
		    jedis.close();
		}
	    }
	}

	for (HostAndPort node : startNodes) {
	    cache.setNodeIfNotExist(node);
	}
    }

    public void renewSlotCache() {
	for (JedisPool jp : cache.getNodes().values()) {
	    Jedis jedis = null;
	    try {
		jedis = jp.getResource();
		cache.discoverClusterSlots(jedis);
		break;
	    } finally {
		if (jedis != null) {
		    jedis.close();
		}
	    }
	}
    }

}
