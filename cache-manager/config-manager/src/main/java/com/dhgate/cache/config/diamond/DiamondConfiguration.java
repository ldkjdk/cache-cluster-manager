package com.dhgate.cache.config.diamond;

import java.util.List;
import java.util.concurrent.Executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dhgate.BaseCachedConfigManager;
import com.dhgate.CacheClusterConfig;
import com.dhgate.CacheConfigurationPaser;
import com.dhgate.ICachedConfigManager;
import com.taobao.diamond.manager.ManagerListener;
import com.taobao.diamond.manager.impl.DefaultDiamondManager;

/**
 * the configuration content from diamond server.
 * 
 * @author  lidingkun
 */
public class DiamondConfiguration extends BaseCachedConfigManager {

    private volatile static ICachedConfigManager cacheManager = null;

    private static final Logger log          = LoggerFactory.getLogger (DiamondConfiguration .class);
   
    private String dataId;
    private String group;
    private DiamondConfiguration(String dataId,String group){
    	this.dataId = dataId.trim();
    	this.group = group.trim();
        init();
    }

    public static ICachedConfigManager getInstance(String dataId,String group) {
        if (cacheManager == null) {
            synchronized (DiamondConfiguration.class) {
                if (cacheManager == null) cacheManager = new DiamondConfiguration(dataId,group);
            }
        }
        return cacheManager;
    }

    
   
   
    /*
     * (non-Javadoc)
     * @see com.alisoft.xplatform.asf.cache.ICacheManager#start()
     */
    public void init() {    
        super.init(); 
    }
    
    private DefaultDiamondManager diamondManager;
    @Override
	public List<com.dhgate.CacheClusterConfig> getCacheClusterConfigs() {
		
		diamondManager = new DefaultDiamondManager(
				group,dataId, new ManagerListener() {
					public Executor getExecutor() {
						return null;
					}

					public void receiveConfigInfo(String newConfigInfo) {
						if (null == newConfigInfo) {
							return;
						}

						List<CacheClusterConfig> rs = CacheConfigurationPaser.loadMemcachedConfig (newConfigInfo);;
						try {
							updateClientPool(rs);
						} catch (Exception ex) {
							log.error("MemcachedManager init error ,please check !");
							throw new RuntimeException("MemcachedManager init error ,please check !",ex);
						}

					}

				});
		
		String configInfo = diamondManager.getAvailableConfigureInfomation(3000);
		if (configInfo != null) {
			List<CacheClusterConfig> clusterConfigs = CacheConfigurationPaser.loadMemcachedConfig(configInfo);

	        if (clusterConfigs != null &&  clusterConfigs.size() > 0) {
	            return clusterConfigs;

	        } else {
	            log.error("no config info for MemcachedManager,please check !");
	            throw new RuntimeException("no config info for MemcachedManager,please check !");
	        }
		}
		
		return null;
	}
   
}
