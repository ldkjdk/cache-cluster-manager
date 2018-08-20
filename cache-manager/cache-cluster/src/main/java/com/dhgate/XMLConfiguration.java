package com.dhgate;

import java.net.URL;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * XMLConfiguration  from local class path loading and  parser the  cache_cluster.xml file.
 * 
 * @author  lidingkun
 */
public class XMLConfiguration extends BaseCachedConfigManager {

    private volatile static ICachedConfigManager cacheManager = null;

    private static final Logger log          = LoggerFactory.getLogger(XMLConfiguration.class);
    
    
    private XMLConfiguration(){
        init();
    }

    public static ICachedConfigManager getInstance() {
        if (cacheManager == null) {
            synchronized (XMLConfiguration.class) {
                if (cacheManager == null) cacheManager = new XMLConfiguration();
            }
        }
        return cacheManager;
    }

    /**
     * configuration file name
     */
    private static final String                             CONFIG_FILE = "cache_cluster.xml";
    private String                                          configFile            = CONFIG_FILE;

    public void init() {
        super.init();
    }
 
 

	@Override
	public List<CacheClusterConfig> getCacheClusterConfigs() {
		try {
            URL url = null;
            ClassLoader loader = Thread.currentThread().getContextClassLoader();
            if (configFile != null && !configFile.equals("")) {
                if (configFile.startsWith("http")) url = new URL(configFile);
                else url = loader.getResource(configFile);
            } else {
                url = loader.getResource(CONFIG_FILE);
            }
            if (url == null) {
                log.error("no cache_cluster configuration find! please put cache_cluster.xml in your classpath");
                throw new RuntimeException("no cache_cluster.xml  found ! please put cache_cluster.xml in your classpath");
            }

            List<CacheClusterConfig> clusterConfigs =  CacheConfigurationPaser.loadMemcachedConfig(url);
            log.info(new StringBuilder().append("load config from :").append(url.getFile()).toString());
            if (clusterConfigs != null && clusterConfigs.size() > 0)
            	return clusterConfigs;
            else
            	throw new RuntimeException("getCacheClusterConfigs is null!");
            
        } catch (Exception ex) {
            log.error("cache_cluster.xml loadConfig error !");
            throw new RuntimeException("cache_cluster.xml loadConfig error !", ex);
        }
	}



	
}
