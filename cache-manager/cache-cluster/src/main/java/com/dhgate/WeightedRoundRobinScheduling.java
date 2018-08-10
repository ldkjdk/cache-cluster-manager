package com.dhgate;

import java.math.BigInteger;
import java.util.List;

/**
  * (WeightedRound-RobinScheduling)-Java
 * @param <T>
  * @@author lidingkun
  * */
public class WeightedRoundRobinScheduling<T> implements RoutingAlg<T>{
 
     private int currentIndex = -1;// last select 
     private int currentWeight = 0;//
     private int maxWeight = 0; // 
     private int gcdWeight = 0; //
     private int serverCount = 0; 
     private int serverWeights[];
     private List<T> serverList;
     private int size =0;
     
     
     public WeightedRoundRobinScheduling(int[] serverWeights, List<T> serverList) {
		super();
		if (serverWeights == null || serverList == null || serverList.size() != serverWeights.length || serverWeights.length<=0) {
			throw new RuntimeException(" WeightedRoundRobinScheduling input parameter is null error" );
		}
		
		this.serverWeights = serverWeights;
		this.serverList = serverList;
		this.size = this.serverList.size();
		this.init();
	}



	/**
      * 返回最大公约数
      * @param a
      * @param b
      * @return
     */
     private  int gcd(int a, int b) {
         BigInteger b1 = new BigInteger(String.valueOf(a));
         BigInteger b2 = new BigInteger(String.valueOf(b));
         BigInteger gcd = b1.gcd(b2);
         return gcd.intValue();
     }
     
      
    
     private int getGCDForServers( ) {
    	 
         int w = 0;
         for (int i = 0,len = serverWeights.length ; i < len - 1; i++) {
             if (w == 0) {
                 w = gcd(serverWeights[i], serverWeights[i + 1]);
             } else {
                 w = gcd(w,serverWeights[i + 1]);
             }
         }
         return w;
     }
     
 
     /**
      * 返回所有服务器中的最大权重
      * @param serverList
      * @return
      */
     private  int getMaxWeightForServers() {
         int w = 0;
         for (int i = 0, len = serverWeights.length; i < len - 1; i++) {
             if (w == 0) {
                 w = Math.max(serverWeights[i], serverWeights[i + 1]);
             } else {
                 w = Math.max(w, serverWeights[i + 1]);
             }
         }
         return w;
     }
    
     public synchronized int getServerIndex () {
         while (true) {
             currentIndex = (currentIndex + 1) % serverCount;
             if (currentIndex == 0) {
                 currentWeight = currentWeight - gcdWeight;
                 if (currentWeight <= 0) {
                     currentWeight = maxWeight;
                     if (currentWeight == 0)
                         return 0;
                 }
             }
             if (serverWeights[currentIndex] >= currentWeight) {
                 return currentIndex;
             }
         }
     }
 
     private void init() {
         currentIndex = -1;
         currentWeight = 0;
         serverCount = this.serverWeights.length;
         maxWeight = getMaxWeightForServers();
         gcdWeight = getGCDForServers();
     }



	@Override
	public  T getServerObject() {
		int index = getServerIndex();
		return serverList.get(index);
	}

	@Override
	public List<T> getAllServerObject() {
		return serverList;
	}

	
	/**
	 * for key hash roundrobin
	 */
	
	@Override
	public T getServerObject(String key) {
		
		Long h = Hashing.MURMUR_HASH.hash(key);
		int index = h.intValue() % size;
		return serverList.get(index);
	}



	@Override
	public T getServerObject(byte[] key) {
		Long h = Hashing.MURMUR_HASH.hash(key);
		int index = h.intValue() % size;
		return serverList.get(index);
	}
 }
