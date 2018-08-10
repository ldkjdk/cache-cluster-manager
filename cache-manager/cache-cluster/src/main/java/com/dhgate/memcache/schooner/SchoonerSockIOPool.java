/*******************************************************************************
 * Copyright (c) 2009 Schooner Information Technology, Inc.
 * All rights reserved.
 * 
 * http://www.schoonerinfotech.com/
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. The name of the author may not be used to endorse or promote products
 *    derived from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
 * THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 ******************************************************************************/
package com.dhgate.memcache.schooner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dhgate.Hashing;
import com.dhgate.memcache.core.PoolConfig;
import com.dhgate.memcache.core.SocketPool;

/*
 * 
 * @author Xingen Wang,lidingkun
 * @since 2.5.0
 * @see SchoonerSockIOPool
 */
public class SchoonerSockIOPool {
	// logger
	
	private static Logger log = LoggerFactory.getLogger(SchoonerSockIOPool.class);
	private static ConcurrentMap<String, SchoonerSockIOPool> pools = new ConcurrentHashMap<String, SchoonerSockIOPool>();

	public static final int NATIVE_HASH = 0; // native String.hashCode();
	public static final int OLD_COMPAT_HASH = 1; // original compatibility
	// hashing algorithm (works with other clients)
	public static final int NEW_COMPAT_HASH = 2; // new CRC32 based
	// compatibility hashing algorithm (works with other clients)
	public static final int CONSISTENT_HASH = 3; // MD5 Based -- Stops
	// thrashing when a server added or removed
	public static final long MAX_RETRY_DELAY = 10 * 60 * 1000;
	// max of 10 minute delay for fall off
	private long maxBusyTime = 1000 * 30; // max idle time for avail sockets
	

	
	
	// connections
	@SuppressWarnings("unused")
	private static int recBufferSize = 128;// bufsize

	// for being alive
	private boolean failover = true; // default to failover in event of cache
	private boolean failback = true; // only used if failover is also set ...
	// controls putting a dead server back
	// into rotation

	// server dead
	// controls putting a dead server back
	// into rotation
	//private boolean nagle = false; // enable/disable Nagle's algorithm
	private int hashingAlg = NATIVE_HASH; // default to using the native hash
	// as it is the fastest

	// locks
	private final ReentrantLock initDeadLock = new ReentrantLock();

	// list of all servers
	private String[] servers;
	private Integer[] weights;
	//private Integer totalWeight = 0;

	private List<String> buckets;
	private TreeMap<Long, String> consistentBuckets;


	private int bufferSize = 1024 * 1025;

	protected SchoonerSockIOPool() {
	}
	
	
	private Map<String, SocketPool> socketPools;
	private final Hashing algo =Hashing.MURMUR_HASH;
	private String poolName = null;
	private PoolConfig config = null;
	private boolean inited = false;
	

	

	public static SchoonerSockIOPool getInstance(String poolName) {
		SchoonerSockIOPool pool;

		synchronized (pools) {
			if (!pools.containsKey(poolName)) {
				pool = new SchoonerSockIOPool();
				pools.putIfAbsent(poolName, pool);
			} else {
				pool = pools.get(poolName);
			}
		}

		return pool;
	}


	/**
	 * Initializes the pool.
	 */
	public void initialize() {
		initDeadLock.lock();
		try {

			// if servers is not set, or it empty, then
			// throw a runtime exception
			if (servers == null || servers.length <= 0) {
				log.error("++++ trying to initialize with no servers");
				throw new IllegalStateException("++++ trying to initialize with no servers");
			}
			// pools
			socketPools = new HashMap<String, SocketPool>(servers.length);
			
			if (this.hashingAlg == CONSISTENT_HASH)
				populateConsistentBuckets();
			else
				populateBuckets();

			inited=true;

		} catch (Exception e) {
			log.error("initialize failed ",e);
		}finally {
			initDeadLock.unlock();
		}

	}
	
	
	
	public void updatePool (String servers[],int hashingAlg,Integer wegits[],PoolConfig config) {
		initDeadLock.lock();
		try {

			if (servers == null || servers.length <= 0) {
				log.error("++++ trying to initialize with no servers");
				throw new IllegalStateException("++++ trying to initialize with no servers");
			}
			
			
			boolean needUp=false;
			if ( servers.length != this.servers.length) {
				needUp = true;
			} else {
				for (int i=0;i<servers.length;i++) {
					if (! servers[i].trim().equals(this.servers[i].trim())) {
						needUp=true;
						break;
					}
				}
			}
			
			
		    if ( needUp || (config.getTimeout() > 0 && config.getTimeout() != this.config.getTimeout())) {
		    	distroyPoll(this.servers);
		    	this.servers=servers;
		    	this.hashingAlg=hashingAlg;
		    	this.weights=wegits;
		    	this.config=config;
		    	
				if (hashingAlg == CONSISTENT_HASH)
					populateConsistentBuckets();
				else
					populateBuckets();
				
				log.info("++++finished updatePool for name " + this.poolName);
		    } else if (config.isDifferent(this.config)) {
		    	this.config=config;
		    	for (SocketPool sp:socketPools.values()) {
		    		sp.setConfig(config);
		    	}
		    } 
		} catch (Exception e) {
			log.error("initialize failed ",e);
		}finally {
			initDeadLock.unlock();
		}

	}
	

	public boolean isTcp() {
		return this.config.isTcp();
	}

	private void populateBuckets() throws Exception {
		
		buckets = new ArrayList<String>();
		for (int i = 0; i < servers.length; i++) {
			String host = servers[i].trim();
			if (this.weights != null && this.weights.length > i) {
				for (int k = 0; k < this.weights[i].intValue(); k++) {
					buckets.add(host);
				}
			} else {
				buckets.add(host);
			}

			SocketPool pool = new SocketPool(config,config.isTcp(),host,config.getTimeout(),config.getConnectTimeout()); 
			socketPools.put(host, pool);
		}
	}

	private void distroyPoll(String[] keys) {

		for (String key : keys) {
			SocketPool pool = socketPools.remove(key);
			try {

				if (pool != null)
					pool.destroy();
				pool = null;

			} catch (Exception e) {
				log.error("distroyPoll :" + key, e);
			}
		}

	}
	
	
	
	private void populateConsistentBuckets() throws Exception {
		// store buckets in tree map
		consistentBuckets = new TreeMap<Long, String>();

		/*if (this.totalWeight <= 0 && this.weights != null) {
			for (int i = 0; i < this.weights.length; i++)
				this.totalWeight += (this.weights[i] == null) ? 1 : this.weights[i];
		} else if (this.weights == null) {
			this.totalWeight = this.servers.length;
		}
*/
		for (int i = 0; i < servers.length; i++) {
			int thisWeight = 1;
			if (this.weights != null && this.weights[i] != null)
				thisWeight = this.weights[i];
				String host = servers[i].trim();
				SocketPool pool = new SocketPool(config,config.isTcp(),host,config.getTimeout(),config.getConnectTimeout());
				
				for (int n = 0; n < 160 * thisWeight; n++) {
					consistentBuckets.put(this.algo.hash("SHARD-" + i + "-NODE-" + n),host);
				}
				
				socketPools.put(host, pool);
		}
	}

	/**
	 * Creates a new SockIO obj for the given server.
	 * 
	 * If server fails to connect, then return null and do not try<br/>
	 * again until a duration has passed. This duration will grow<br/>
	 * by doubling after each failed attempt to connect.
	 * 
	 * @param host
	 *            host:port to connect to
	 * @return SockIO obj or null if failed to create
	 */
	



	/**
	 * Gets the host that a particular key / hashcode resides on.
	 * 
	 * @param key
	 * @return
	 */
	public final String getHost(String key) {
		return getHost(key, null);
	}

	/**
	 * Gets the host that a particular key / hashcode resides on.
	 * 
	 * @param key
	 * @param hashcode
	 * @return
	 */
	public final String getHost(String key, Integer hashcode) {
		SchoonerSockIO socket;
		try {
			socket = getSock(key);
			if (socket != null) {
				String host = socket.getHost();
				socket.close();
				return host;
			}
		} catch (Exception e) {
			log.error("getHost",e);
		}
		
		return null;
	}

	/**
	 * Returns appropriate SockIO object given string cache key and optional
	 * hashcode.
	 * 
	 * Trys to get SockIO from pool. Fails over to additional pools in event of
	 * server failure.
	 * 
	 * @param key
	 *            hashcode for cache key
	 * @param hashCode
	 *            if not null, then the int hashcode to use
	 * @return SockIO obj connected to server
	 * @throws Exception 
	 */
	public final SchoonerSockIO getSock(String key)  {

		// if no servers return null
		int size = 0;
		if ((this.hashingAlg == CONSISTENT_HASH && (size = consistentBuckets.size()) == 0) || (buckets != null && (size = buckets.size()) == 0))
			return null;
		else if (size == 1) {
			SchoonerSockIO sock = (this.hashingAlg == CONSISTENT_HASH) ? getConnection(consistentBuckets.get(consistentBuckets.firstKey())) : getConnection(buckets.get(0));
			return sock;
		}

		
		//Set<String> tryServers = new HashSet<String>(Arrays.asList(servers));
		// get initial bucket
		long bucket = getBucket(key);
		String server = (this.hashingAlg == CONSISTENT_HASH) ? consistentBuckets.get(bucket) : buckets.get((int) bucket);
		SchoonerSockIO sock = getConnection(server);
		
		
		return sock;
	}

	
	public final SchoonerSockIO getSock(String key,Integer hashCode) {
		return getSock (key);
	}
	/**
	 * Returns a SockIO object from the pool for the passed in host.
	 * 
	 * Meant to be called from a more intelligent method<br/>
	 * which handles choosing appropriate server<br/>
	 * and failover.
	 * 
	 * @param host
	 *            host from which to retrieve object
	 * @return SockIO object or null if fail to retrieve one
	 * @throws Exception 
	 */
	public final SchoonerSockIO getConnection(String host)  {
		
		if (host == null)
			return null;
		// if we have items in the pool then we can return it
		SocketPool sockets = socketPools.get(host);
		SchoonerSockIO socket = null;
		try {
			socket = sockets.getResource();
		} catch (Exception e) {
			log.error("getConnection", e);
		}
		return socket;
	}

	protected final void closeSocketPool() {
		for (SocketPool i : socketPools.values()) {
			try {
				i.destroy();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				log.error("closeSocketPool", e);
			}
		}
		inited=false;
	}

	/**
	 * Shuts down the pool.
	 * 
	 * Cleanly closes all sockets.<br/>
	 * Stops the maint thread.<br/>
	 * Nulls out all internal maps<br/>
	 */
	public void shutDown() {
		closeSocketPool();

		socketPools.clear();
		socketPools = null;
		buckets = null;
		consistentBuckets = null;
		

	}

	
	/**
	 * Sets the list of all cache servers.
	 * 
	 * @param servers
	 *            String array of servers [host:port]
	 */
	public final void setServers(String[] servers) {
		this.servers = servers;
	}

	/**
	 * Returns the current list of all cache servers.
	 * 
	 * @return String array of servers [host:port]
	 */
	public final String[] getServers() {
		return this.servers;
	}

	/**
	 * Sets the list of weights to apply to the server list.
	 * 
	 * This is an int array with each element corresponding to an element<br/>
	 * in the same position in the server String array.
	 * 
	 * @param weights
	 *            Integer array of weights
	 */
	public final void setWeights(Integer[] weights) {
		this.weights = weights;
	}

	/**
	 * Returns the current list of weights.
	 * 
	 * @return int array of weights
	 */
	public final Integer[] getWeights() {
		return this.weights;
	}
	

	/**
	 * Sets the failover flag for the pool.
	 * 
	 * If this flag is set to true, and a socket fails to connect,<br/>
	 * the pool will attempt to return a socket from another server<br/>
	 * if one exists. If set to false, then getting a socket<br/>
	 * will return null if it fails to connect to the requested server.
	 * 
	 * @param failover
	 *            true/false
	 */
	public final void setFailover(boolean failover) {
		this.failover = failover;
	}

	/**
	 * Returns current state of failover flag.
	 * 
	 * @return true/false
	 */
	public final boolean getFailover() {
		return this.failover;
	}

	/**
	 * Sets the failback flag for the pool.
	 * 
	 * If this is true and we have marked a host as dead, will try to bring it
	 * back. If it is false, we will never try to resurrect a dead host.
	 * 
	 * @param failback
	 *            true/false
	 */
	public void setFailback(boolean failback) {
		this.failback = failback;
	}

	/**
	 * Returns current state of failover flag.
	 * 
	 * @return true/false
	 */
	public boolean getFailback() {
		return this.failback;
	}


	/**
	 * Sets the Nagle alg flag for the pool.
	 * 
	 * If false, will turn off Nagle's algorithm on all sockets created.
	 * 
	 * @param nagle
	 *            true/false
	 */
	public final void setNagle(boolean nagle) {
		this.config.setNagle(nagle);
	}

	/**
	 * Returns current status of nagle flag
	 * 
	 * @return true/false
	 */
	public final boolean getNagle() {
		return this.config.isNagle();
	}

	public final void setHashingAlg(int alg) {
		this.hashingAlg = alg;
	}

	/**
	 * Returns current status of customHash flag
	 * 
	 * @return true/false
	 */
	public final int getHashingAlg() {
		return this.hashingAlg;
	}

	private final long getBucket(String key) {
		long hc = algo.hash(key);
		if (this.hashingAlg == CONSISTENT_HASH) {
			SortedMap<Long, String> tail = consistentBuckets.tailMap(hc);

			if (tail.isEmpty()) {
				return consistentBuckets.firstKey();
			}
			
			return tail.firstKey();
					
		} else {
			long bucket = hc % buckets.size();
			if (bucket < 0)
				bucket *= -1;
			return bucket;
		}
	}

	public void setBufferSize(int bufferSize) {
		this.bufferSize = bufferSize;
	}

	public int getBufferSize() {
		return bufferSize;
	}

	public long getMaxBusy() {
		return maxBusyTime;
	}

	public void setMaxBusy(long maxBusyTime) {
		this.maxBusyTime = maxBusyTime;
	}

	public String getPoolName() {
		return poolName;
	}

	public void setPoolName(String poolName) {
		this.poolName = poolName;
	}
	

	public boolean isInited() {
		return inited;
	}

	public PoolConfig getConfig() {
		return config;
	}

	public void setConfig(PoolConfig config) {
		this.config = config;
	}
	
	public boolean ping() throws Exception {
		if (socketPools == null) return false;
		
		for ( SocketPool sp: socketPools.values()) {
			SchoonerSockIO io = sp.getResource();
			boolean r =  io.isAlive();
			io.close();
			if (r == false) {
				log.error("ping failed for " + sp.getHost());
				return false;
			}
		}
		
		return true;
	}
}
