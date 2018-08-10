package com.dhgate.redis.clients.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.dhgate.Hashing;
import com.dhgate.SafeEncoder;

public class Sharded<R, S extends ShardInfo<R>> {

    public static final int DEFAULT_WEIGHT = 1;
    private TreeMap<Long, S> nodes;
    private final Hashing algo;
    private final Map<ShardInfo<R>, R> resources = new LinkedHashMap<ShardInfo<R>, R>();
    private boolean polling = false;
    private List<S> buckets;
    /**
     * The default pattern used for extracting a key tag. The pattern must have
     * a group (between parenthesis), which delimits the tag to be hashed. A
     * null pattern avoids applying the regular expression for each lookup,
     * improving performance a little bit is key tags aren't being used.
     */
    private Pattern tagPattern = null;
    // the tag is anything between {}
    public static final Pattern DEFAULT_KEY_TAG_PATTERN = Pattern
	    .compile("\\{(.+?)\\}");

    public Sharded(List<S> shards,boolean isPolling) {
	this(shards, Hashing.MURMUR_HASH,isPolling); // MD5 is really not good as we works
	// with 64-bits not 128
    }

    public Sharded(List<S> shards, Hashing algo,boolean isPolling) {
	this.algo = algo;
	this.polling=isPolling;
	initialize(shards);
    }

    public Sharded(List<S> shards, Pattern tagPattern,boolean isPolling) {
	this(shards, Hashing.MURMUR_HASH, tagPattern,isPolling ); // MD5 is really not good
	// as we works with
	// 64-bits not 128
    }

    public Sharded(List<S> shards, Hashing algo, Pattern tagPattern,boolean isPolling) {
	this.algo = algo;
	this.tagPattern = tagPattern;
	this.polling = isPolling;
	initialize(shards);
    }

	private void initialize(List<S> shards) {
		if (this.polling) {
			buckets = new ArrayList<S> ();
			for (int i = 0; i != shards.size(); ++i) {
				final S shardInfo = shards.get(i);
				buckets.add(shardInfo);
				resources.put(shardInfo, shardInfo.createResource());
			}
			
		} else {
			nodes = new TreeMap<Long, S>();

			for (int i = 0; i != shards.size(); ++i) {
				final S shardInfo = shards.get(i);
				if (shardInfo.getName() == null)
					for (int n = 0; n < 160 * shardInfo.getWeight(); n++) {
						nodes.put(this.algo.hash("SHARD-" + i + "-NODE-" + n),shardInfo);
					}
				else
					for (int n = 0; n < 160 * shardInfo.getWeight(); n++) {
						nodes.put(this.algo.hash(shardInfo.getName() + "*"+ shardInfo.getWeight() + n), shardInfo);
					}
				resources.put(shardInfo, shardInfo.createResource());
			}
		}
	}

    public R getShard(byte[] key) {
	return resources.get(getShardInfo(key));
    }

    public R getShard(String key) {
	return resources.get(getShardInfo(key));
    }

    public S getShardInfo(byte[] key) {
    if (this.polling) {
    	Long kl = algo.hash(key);
    	int bucket = kl.intValue() % buckets.size();
		if (bucket < 0)
			bucket *= -1;
		
		  return buckets.get(bucket);
    } else {
		SortedMap<Long, S> tail = nodes.tailMap(algo.hash(key));
		if (tail.isEmpty()) {
		    return nodes.get(nodes.firstKey());
		}
		return tail.get(tail.firstKey());
    }
    }

    public S getShardInfo(String key) {
	return getShardInfo(SafeEncoder.encode(getKeyTag(key)));
    }

    /**
     * A key tag is a special pattern inside a key that, if preset, is the only
     * part of the key hashed in order to select the server for this key.
     * 
     * @see http://code.google.com/p/redis/wiki/FAQ#I
     *      'm_using_some_form_of_key_hashing_for_partitioning,_but_wh
     * @param key
     * @return The tag if it exists, or the original key
     */
    public String getKeyTag(String key) {
	if (tagPattern != null) {
	    Matcher m = tagPattern.matcher(key);
	    if (m.find())
		return m.group(1);
	}
	return key;
    }

    public Collection<S> getAllShardInfo() {
	return Collections.unmodifiableCollection(nodes.values());
    }

    public Collection<R> getAllShards() {
	return Collections.unmodifiableCollection(resources.values());
    }
}
