package com.dhgate;

import java.io.CharArrayReader;
import java.io.Reader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.dom4j.Document;
import org.dom4j.DocumentFactory;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author  lidingkun
 */
public class CacheConfigurationPaser {

    private static final Logger log = LoggerFactory.getLogger(CacheConfigurationPaser.class);

  
    public static List<CacheClusterConfig> loadMemcachedConfig (URL url) {
        
    	List<CacheClusterConfig> clusterConfigs = new ArrayList<CacheClusterConfig> ();
        try {
            DocumentFactory df = DocumentFactory.getInstance();
            
            SAXReader reader = new SAXReader(df);
            
            Document document = reader.read(url);
            clusterConfigs = loadMemcachedConfig(document);
        } catch (Exception e) {
            log.error(new StringBuilder("MemcachedManager loadConfig error !").append(" config url :").append(url.toString()).toString(),e);
           
        }
        
        return clusterConfigs;
    }
    
    public static List<CacheClusterConfig> loadMemcachedConfig (String cfg) {
        
    	List<CacheClusterConfig> clusterConfigs = new ArrayList<CacheClusterConfig> ();
        try {
        	DocumentFactory df = DocumentFactory.getInstance();
            
            SAXReader reader = new SAXReader(df);
            
            Reader r = new CharArrayReader(cfg.toCharArray());
            Document document = reader.read(r);
            clusterConfigs = loadMemcachedConfig(document);
        } catch (Exception e) {
            log.error(new StringBuilder("MemcachedManager loadConfig error !").append("content :").append(cfg).toString(),e);
           
        }
        
        return clusterConfigs;
    }
    
    
    public static List<CacheClusterConfig> loadMemcachedConfig (Document document) {
        
    	List<CacheClusterConfig> clusterConfigs = new ArrayList<CacheClusterConfig> ();
    
            Element root = document.getRootElement();
            for (Iterator<Element> i = root.elementIterator(); i.hasNext();) {
                Element clusterElement =  i.next();
                
                if (ICachedConfigManager.CLUSTER.equalsIgnoreCase(clusterElement.getName())) {
                	CacheClusterConfig clusterConfig = new CacheClusterConfig ();
                	clusterConfig.setName(clusterElement.attribute(ICachedConfigManager.CLUSTER_NAME).getValue());
                	if (clusterElement.attribute(ICachedConfigManager.CLUSTER_FAILOVER) != null) clusterConfig.setFailover(Boolean.parseBoolean(clusterElement.attribute(ICachedConfigManager.CLUSTER_FAILOVER).getValue()));
                	if (clusterElement.attribute(ICachedConfigManager.CLUSTER_ALG) != null) clusterConfig.setClusterAlg(clusterElement.attribute(ICachedConfigManager.CLUSTER_ALG).getValue());
                	if (clusterElement.attribute(ICachedConfigManager.CLUSTER_TYPE) != null) clusterConfig.setClusterType (clusterElement.attribute(ICachedConfigManager.CLUSTER_TYPE).getValue());
                	if (clusterElement.attribute(ICachedConfigManager.CLUSTER_ASYNC) != null) clusterConfig.setAsyn(Boolean.parseBoolean(clusterElement.attribute(ICachedConfigManager.CLUSTER_ASYNC).getValue()));
                	
                	
                	List<CacheConfig> clientList = new ArrayList <CacheConfig> ();
                	
                	for (Iterator<Element> ci=clusterElement.elementIterator();ci.hasNext();) {
                		Element client = ci.next();
                		if (ICachedConfigManager.CLUSTER_CLIENT.equalsIgnoreCase(client.getName())) {
                			CacheConfig clientConfig =new CacheConfig();
                			//clientConfig.setBinary(isBinary)
                			
                			if (client.attribute(ICachedConfigManager.CLUSTER_CLIENT_PROTOCOL) != null) clientConfig.setProtocol(client.attribute(ICachedConfigManager.CLUSTER_CLIENT_PROTOCOL).getValue());
                			if (client.attribute(ICachedConfigManager.CLUSTER_CLIENT_WEIGHT) != null) clientConfig.setWeight(Integer.parseInt(client.attribute(ICachedConfigManager.CLUSTER_CLIENT_WEIGHT).getValue()));
                			clientConfig.setName(clusterConfig.getName()+"."+client.attribute(ICachedConfigManager.CLUSTER_NAME).getValue()+"."+clientConfig.getProtocol());
                			
                			if (client.attribute(ICachedConfigManager.CLUSTER_CLIENT_SLAVE) != null) clientConfig.setSlave(Boolean.parseBoolean(client.attribute(ICachedConfigManager.CLUSTER_CLIENT_SLAVE).getValue()));
                			if (client.attribute(ICachedConfigManager.CLUSTER_CLIENT_SENTINEL) != null){
                				clientConfig.setSentinel(Boolean.parseBoolean(client.attribute(ICachedConfigManager.CLUSTER_CLIENT_SENTINEL).getValue()));
                				clientConfig.setSentinelName(client.attribute(ICachedConfigManager.CLUSTER_SENTINELMASTERNAME).getValue());
                			}
                			
                			/*Element er = client.element("errorHandler");
                			if (er != null)
                			    clientConfig.setErrorHandler(er.getText());*/
                			
                			Element poll = client.element(ICachedConfigManager.POOL_TAG);
                			
                			if (poll != null) {
                				SocketPoolConfig sc = new SocketPoolConfig ();
                				sc.setName(clientConfig.getName());
                				if (poll.attribute(ICachedConfigManager.POOL_TCP) != null) sc.setTcp(Boolean.parseBoolean(poll.attribute(ICachedConfigManager.POOL_TCP).getValue()));

                                if (poll.attribute(ICachedConfigManager.POOL_INITCONN) != null) sc.setInitConn(Integer.parseInt(poll.attribute(ICachedConfigManager.POOL_INITCONN).getValue()));

                                if (poll.attribute(ICachedConfigManager.POOL_MINIDLE) != null) sc.setMinIdle(Integer.parseInt(poll.attribute(ICachedConfigManager.POOL_MINIDLE).getValue()));

                                if (poll.attribute(ICachedConfigManager.POOL_MAXACTIVE) != null) sc.setMaxActive(Integer.parseInt(poll.attribute(ICachedConfigManager.POOL_MAXACTIVE).getValue()));

                                if (poll.attribute(ICachedConfigManager.POOL_TIMEBETWEENEVICTIONRUNSMILLIS) != null) sc.setTimeBetweenEvictionRunsMillis(Integer.parseInt(poll.attribute(ICachedConfigManager.POOL_TIMEBETWEENEVICTIONRUNSMILLIS).getValue()));

                               // if (poll.attribute("nagle") != null) sc.setNagle(Boolean.parseBoolean(poll.attribute("nagle").getValue()));

                                if (poll.attribute(ICachedConfigManager.READ_TIMEOUT) != null) sc.setTimeout(Integer.parseInt(poll.attribute(ICachedConfigManager.READ_TIMEOUT).getValue()));


                                if (poll.attribute(ICachedConfigManager.POOL_TESTONBORROW) != null) sc.setTestOnBorrow(Boolean.parseBoolean(poll.attribute(ICachedConfigManager.POOL_TESTONBORROW).getValue()));

                                if (poll.attribute(ICachedConfigManager.POOL_MAXBUSYTIME) != null) sc.setMaxBusyTime(Long.parseLong(poll.attribute(ICachedConfigManager.POOL_MAXBUSYTIME).getValue()));

                                if (poll.attribute(ICachedConfigManager.HASHING_ALGP) != null) sc.setHashingAlg(Integer.parseInt(poll.attribute(ICachedConfigManager.HASHING_ALGP).getValue()));

                                if (poll.attribute(ICachedConfigManager.CONNECT_TIMEOUT) != null) sc.setConnectTimeout(Integer.parseInt(poll.attribute(ICachedConfigManager.CONNECT_TIMEOUT).getValue()));
                                
                                if (poll.attribute(ICachedConfigManager.POOL_MAXWAIT) != null) sc.setMaxWait(Integer.parseInt(poll.attribute(ICachedConfigManager.POOL_MAXWAIT).getValue()));
                                
                                if (poll.attribute(ICachedConfigManager.POOL_NUMTESTSPEREVICTIONRUN) != null) sc.setNumTestsPerEvictionRun(Integer.parseInt(poll.attribute(ICachedConfigManager.POOL_NUMTESTSPEREVICTIONRUN).getValue()));
                                if (poll.attribute(ICachedConfigManager.POOL_MINEVICTABLEIDLETIMEMILLIS) != null) sc.setMinEvictableIdleTimeMillis(Integer.parseInt(poll.attribute(ICachedConfigManager.POOL_MINEVICTABLEIDLETIMEMILLIS).getValue()));
                                
                                sc.setServers(poll.elementText(ICachedConfigManager.POOL_SERVERS));
                                if (poll.element(ICachedConfigManager.POOL_WEIGHTS) != null) sc.setWeights(poll.elementText(ICachedConfigManager.POOL_WEIGHTS));
                                
                				clientConfig.setSocketPool(sc);
                			}
                			
                			clientList.add(clientConfig);
                		}
                	}
                	
                	clusterConfig.setClients(clientList.toArray(new CacheConfig[0]));
                	clusterConfigs.add(clusterConfig);
                }                
            }
        
        
        return clusterConfigs;
    }
    
    
}
