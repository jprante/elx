package org.xbib.elx.transport;

import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.xbib.elx.common.AbstractBulkClient;

/**
 * Transport search client with additional methods.
 */
public class TransportBulkClient extends AbstractBulkClient {

    private final TransportClientHelper helper;

    public TransportBulkClient() {
        super();
        this.helper = new TransportClientHelper();
    }

    @Override
    public ElasticsearchClient createClient(Settings settings) {
        return helper.createClient(settings);
    }

    @Override
    public void init(Settings settings) {
        super.init(settings);
        helper.init((TransportClient) getClient(), settings);
    }

    @Override
    public void closeClient(Settings settings) {
        helper.closeClient(settings);
    }
}
