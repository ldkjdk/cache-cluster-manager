package com.dhgate.memcache;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;

/**
 *
 * 
 * @author  lidingkun
 */
public class ClusterProcessorManager {

    private static ClusterProcessorManager cpm             = new ClusterProcessorManager();
    private final static int               CORE_POOL_SIZE  = 10;
    private final static int               MAX_POOL_SIZE   = 30;
    private final static int               KEEP_ALIVE_TIME = 10;
    private final static int               WORK_QUEUE_SIZE = 3000;
    final ThreadPoolExecutor               threadPool      = new ThreadPoolExecutor(CORE_POOL_SIZE, MAX_POOL_SIZE, KEEP_ALIVE_TIME,
                                                                                    TimeUnit.SECONDS,
                                                                                    new LinkedBlockingQueue<Runnable>(WORK_QUEUE_SIZE),
                                                                                    new ThreadPoolExecutor.CallerRunsPolicy());
    
    private ClusterProcessorManager() {
    	this.threadPool.setRejectedExecutionHandler(new CallerRunsPolicy());
    }

    public static ClusterProcessorManager newInstance() {
        return cpm;
    }

   /* public void asynProcess(Object[] commands) {
        Runnable task = new ClusterProcessor(commands);
        threadPool.execute(task);
    }*/
    
    public void exec (Runnable run) {
    	threadPool.execute(run);
    }
}
