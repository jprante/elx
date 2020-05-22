package org.xbib.elx.common;

import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.settings.Settings;
import org.xbib.elx.api.NativeClient;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class MockNativeClient extends AbstractNativeClient implements NativeClient {

    @Override
    protected void ensureClientIsPresent() {
    }

    @Override
    public void setClient(ElasticsearchClient client) {
    }

    @Override
    public ElasticsearchClient getClient() {
        return null;
    }

    @Override
    protected ElasticsearchClient createClient(Settings settings) throws IOException {
        return null;
    }

    @Override
    protected void closeClient() throws IOException {

    }

    @Override
    public void init(Settings settings) throws IOException {

    }

    @Override
    public String getClusterName() {
        return null;
    }

    @Override
    public String getHealthColor(long maxWaitTime, TimeUnit timeUnit) {
        return null;
    }

    @Override
    public void waitForCluster(String healthColor, long maxWaitTime, TimeUnit timeUnit) {

    }

    @Override
    public void waitForShards(long maxWaitTime, TimeUnit timeUnit) {

    }

    @Override
    public long getSearchableDocs(String index) {
        return 0;
    }

    @Override
    public boolean isIndexExists(String index) {
        return false;
    }

    @Override
    public void close() throws IOException {

    }
}
