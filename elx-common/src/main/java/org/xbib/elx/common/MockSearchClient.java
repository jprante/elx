package org.xbib.elx.common;

import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.settings.Settings;

/**
 * A mocked client, it does not perform any actions on a cluster. Useful for testing.
 */
public class MockSearchClient extends AbstractSearchClient {

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
    public void close() {
        // nothing to do
    }
}
