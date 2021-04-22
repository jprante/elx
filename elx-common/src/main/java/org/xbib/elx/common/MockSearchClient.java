package org.xbib.elx.common;

import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.settings.Settings;

import java.util.concurrent.TimeUnit;

/**
 * A mocked client, it does not perform any actions on a cluster. Useful for testing.
 */
public class MockSearchClient extends AbstractSearchClient {

    @Override
    public ElasticsearchClient getClient() {
        return null;
    }

    @Override
    public void init(Settings settings) {
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
    public void close() {
        // nothing to do
    }
}
