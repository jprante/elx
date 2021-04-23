package org.xbib.elx.common;

import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.settings.Settings;

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
    protected void closeClient(Settings settings) {
    }

    @Override
    public void close() {
        // nothing to do
    }
}
