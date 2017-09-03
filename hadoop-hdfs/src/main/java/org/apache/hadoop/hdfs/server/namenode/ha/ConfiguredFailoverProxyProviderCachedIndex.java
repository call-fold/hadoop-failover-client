package org.apache.hadoop.hdfs.server.namenode.ha;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.LinkedBlockingQueue;

public class ConfiguredFailoverProxyProviderCachedIndex {

    private static final Log LOG = LogFactory.getLog(ConfiguredFailoverProxyProviderCached.class);

    private static LinkedBlockingQueue<Integer> indexQueue = new LinkedBlockingQueue<Integer>(1);

    public static void setIndex(int index) {
        LOG.debug("Begin to set cached namenode index: " + index);
        if (!indexQueue.isEmpty()) {
            indexQueue.clear();
        }
        indexQueue.offer(index);
    }

    public static int getIndex() {
        int index = 0;
        if (!indexQueue.isEmpty()) {
            LOG.debug("Begin to read cached namenode index");
            index = indexQueue.peek();
        }
        LOG.debug("namenode index is: " + index);
        return index;
    }

}
