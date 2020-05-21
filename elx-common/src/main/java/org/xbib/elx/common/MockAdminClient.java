package org.xbib.elx.common;

import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.settings.Settings;

import java.util.concurrent.TimeUnit;

/**
 * A mocked client, it does not perform any actions on a cluster. Useful for testing.
 */
public class MockAdminClient extends AbstractAdminClient {

    @Override
    public ElasticsearchClient getClient() {
        return null;
    }

    @Override
    public void init(Settings settings) {
    }

    @Override
    protected ElasticsearchClient createClient(Settings settings) {
        return null;
    }

    @Override
    protected void closeClient() {
    }

    @Override
    public MockAdminClient deleteIndex(String index) {
        return this;
    }

    @Override
    public boolean forceMerge(String index, long maxWaitTime, TimeUnit timeUnit) {
        return true;
    }

    @Override
    public void waitForCluster(String healthColor, long timeValue, TimeUnit timeUnit) {
    }

    @Override
    public boolean waitForRecovery(String index, long maxWaitTime, TimeUnit timeUnit) {
        return true;
    }

    @Override
    public MockAdminClient updateReplicaLevel(String index, int level, long maxWaitTime, TimeUnit timeUnit) {
        return this;
    }

    @Override
    public void close() {
        // nothing to do
    }
}
