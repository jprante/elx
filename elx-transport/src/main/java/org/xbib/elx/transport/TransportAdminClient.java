package org.xbib.elx.transport;

import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.xbib.elx.common.AbstractAdminClient;

import java.io.IOException;

/**
 * Transport admin client.
 */
public class TransportAdminClient extends AbstractAdminClient {

    private final TransportClientHelper helper;

    public TransportAdminClient() {
        this.helper = new TransportClientHelper();
    }

    @Override
    public ElasticsearchClient createClient(Settings settings) throws IOException {
        return helper.createClient(settings);
    }

    @Override
    public void init(Settings settings) throws IOException {
        super.init(settings);
        helper.init((TransportClient) getClient(), settings);
    }

    @Override
    public void closeClient(Settings settings) {
        helper.closeClient(settings);
    }
}
