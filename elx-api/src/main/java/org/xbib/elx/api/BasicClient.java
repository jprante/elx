package org.xbib.elx.api;

import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.settings.Settings;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public interface BasicClient extends Closeable {

    void putClusterSetting(String key, Object value, long timeout, TimeUnit timeUnit) throws IOException;

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
     * Initiative the extended client, the bulk metric and bulk controller,
     * creates instances and connect to cluster, if required.
     *
     * @param settings settings
     * @throws IOException if init fails
     */
    void init(Settings settings) throws IOException;

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
     *
     * @param healthColor cluster health color to wait for
     * @param maxWaitTime   time value
     * @param timeUnit time unit
     */
    void waitForCluster(String healthColor, long maxWaitTime, TimeUnit timeUnit);

    void waitForShards(long maxWaitTime, TimeUnit timeUnit);

    long getSearchableDocs(IndexDefinition indexDefinition);

    boolean isIndexExists(IndexDefinition indexDefinition);

    ScheduledThreadPoolExecutor getScheduler();
}
