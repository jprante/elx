package org.xbib.elx.common;

import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.settings.Settings;

import java.util.concurrent.TimeUnit;

/**
 * A mocked client, it does not perform any actions on a cluster. Useful for testing.
 */
public class MockExtendedClient extends AbstractExtendedClient {

    @Override
    public ElasticsearchClient getClient() {
        return null;
    }

    @Override
    public MockExtendedClient init(Settings settings) {
        return this;
    }

    @Override
    protected ElasticsearchClient createClient(Settings settings) {
        return null;
    }

    @Override
    protected void closeClient() {
    }

    @Override
    public MockExtendedClient index(String index, String id, boolean create, String source) {
        return this;
    }

    @Override
    public MockExtendedClient delete(String index, String id) {
        return this;
    }

    @Override
    public MockExtendedClient update(String index, String id, String source) {
        return this;
    }

    @Override
    public MockExtendedClient index(IndexRequest indexRequest) {
        return this;
    }

    @Override
    public MockExtendedClient delete(DeleteRequest deleteRequest) {
        return this;
    }

    @Override
    public MockExtendedClient update(UpdateRequest updateRequest) {
        return this;
    }

    @Override
    public MockExtendedClient startBulk(String index, long startRefreshInterval, long stopRefreshIterval) {
        return this;
    }

    @Override
    public MockExtendedClient stopBulk(String index, long maxWaitTime, TimeUnit timeUnit) {
        return this;
    }

    @Override
    public MockExtendedClient newIndex(String index) {
        return this;
    }

    @Override
    public MockExtendedClient deleteIndex(String index) {
        return this;
    }

    @Override
    public MockExtendedClient refreshIndex(String index) {
        return this;
    }

    @Override
    public MockExtendedClient flushIndex(String index) {
        return this;
    }

    @Override
    public boolean forceMerge(String index, long maxWaitTime, TimeUnit timeUnit) {
        return true;
    }

    @Override
    public boolean waitForCluster(String healthColor, long timeValue, TimeUnit timeUnit) {
        return true;
    }

    @Override
    public boolean waitForResponses(long maxWaitTime, TimeUnit timeUnit) {
        return true;
    }

    @Override
    public boolean waitForRecovery(String index, long maxWaitTime, TimeUnit timeUnit) {
        return true;
    }

    @Override
    public MockExtendedClient updateReplicaLevel(String index, int level, long maxWaitTime, TimeUnit timeUnit) {
        return this;
    }

    @Override
    public void flush() {
        // nothing to do
    }

    @Override
    public void close() {
        // nothing to do
    }
}
