package org.xbib.elx.transport;

import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.xbib.elx.common.AbstractSearchClient;
import java.io.IOException;

/**
 * Transport search client with additional methods.
 */
public class TransportSearchClient extends AbstractSearchClient {

    private final TransportClientHelper helper;

    public TransportSearchClient() {
        super();
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
