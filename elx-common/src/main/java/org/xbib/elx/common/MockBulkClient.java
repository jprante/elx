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
public class MockBulkClient extends AbstractBulkClient {

    @Override
    public ElasticsearchClient getClient() {
        return null;
    }

    @Override
    public boolean init(Settings settings, String info) {
        return true;
    }

    @Override
    public String getClusterName() {
        return null;
    }

    @Override
    protected ElasticsearchClient createClient(Settings settings) {
        return null;
    }

    @Override
    protected void closeClient(Settings settings) {
    }

    @Override
    public MockBulkClient index(IndexRequest indexRequest) {
        return this;
    }

    @Override
    public MockBulkClient delete(DeleteRequest deleteRequest) {
        return this;
    }

    @Override
    public MockBulkClient update(UpdateRequest updateRequest) {
        return this;
    }

    @Override
    public boolean waitForResponses(long maxWaitTime, TimeUnit timeUnit) {
        return true;
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
