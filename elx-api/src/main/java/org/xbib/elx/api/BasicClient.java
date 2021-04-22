package org.xbib.elx.api;

import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.settings.Settings;
import java.io.Closeable;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public interface BasicClient extends Closeable {

    /**
     * Initiative the client
     * @param settings settings
     */
    void init(Settings settings);

    void putClusterSetting(String key, Object value, long timeout, TimeUnit timeUnit);

    /**
     * Set an Elasticsearch client to extend from it. May be null for TransportClient.
     * @param client the Elasticsearch client
     */
    void setClient(ElasticsearchClient client);

    /**
     * Return Elasticsearch client.
     *
     * @return Elasticsearch client
     */
    ElasticsearchClient getClient();


    /**
     * Get cluster name.
     * @return the cluster name
     */
    String getClusterName();

    /**
     * Get current health color.
     *
     * @param maxWaitTime maximum wait time
     * @param timeUnit time unit
     * @return the cluster health color
     */
    String getHealthColor(long maxWaitTime, TimeUnit timeUnit);

    /**
     * Wait for cluster being healthy.
     */
    void waitForHealthyCluster();

    long getSearchableDocs(IndexDefinition indexDefinition);

    boolean isIndexExists(IndexDefinition indexDefinition);

    ScheduledThreadPoolExecutor getScheduler();
}
