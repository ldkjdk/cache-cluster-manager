package com.dhgate;

import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * 
 * @author lidingkun
 *
 */
public class ConsistentHash<T> implements RoutingAlg<T>{
	private final Hashing algo =Hashing.MURMUR_HASH;
	private TreeMap<Long, String> consistentBuckets;
	private int serverWeights[];
    private Map<String, T> serverList;
	
	public ConsistentHash(int[] serverWeights, Map<String, T> serverList) {
		super();
		if (serverWeights == null || serverList == null || serverList.size() != serverWeights.length || serverWeights.length<=0) {
			throw new RuntimeException(" WeightedRoundRobinScheduling input parameter is null error" );
		}
		
		this.serverWeights = serverWeights;
		this.serverList = serverList;
		
		consistentBuckets= new TreeMap<Long, String> ();
		this.init();
	}

	private void init() {
		String keys [] = serverList.keySet().toArray(new String[0]);
		
		for (int i = 0; i < keys.length; i++) {
			int thisWeight = 1;
			if (this.serverWeights[i] > 0 )
				thisWeight = this.serverWeights[i];
			
				String host = keys[i];
				
				
				for (int n = 0; n < 160 * thisWeight; n++) {
					consistentBuckets.put(this.algo.hash("SHARD-" + i + "-NODE-" + n),host);
				}
		}
		
	}
	
	@Override
	public T getServerObject() {
		throw new RuntimeException(" ConsistentHash not support #getServerObject()" );
	}

	@Override
	public List<T> getAllServerObject() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public T getServerObject(String key) {
		long hc = algo.hash(key);
		Long k = null;
			SortedMap<Long, String> tail = consistentBuckets.tailMap(hc);

			if (tail.isEmpty()) {
				k = consistentBuckets.firstKey();
			} else {
				k = tail.firstKey();
			}
			
         return serverList.get(consistentBuckets.get(k));
	}

	@Override
	public T getServerObject(byte[] key) {
		long hc = algo.hash(key);
		Long k = null;
			SortedMap<Long, String> tail = consistentBuckets.tailMap(hc);

			if (tail.isEmpty()) {
				k = consistentBuckets.firstKey();
			} else {
				k = tail.firstKey();
			}
			
         return serverList.get(consistentBuckets.get(k));
	}

	@Override
	public int getServerIndex() {
		// TODO Auto-generated method stub
		return 0;
	}
}
