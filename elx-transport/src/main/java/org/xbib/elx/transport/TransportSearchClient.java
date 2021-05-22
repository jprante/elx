package org.xbib.elx.transport;

import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.xbib.elx.common.AbstractSearchClient;

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
    public ElasticsearchClient createClient(Settings settings) {
        return helper.createClient(settings);
    }

    @Override
    public boolean init(Settings settings, String info) {
        if (super.init(settings, "Netty: " + io.netty.util.Version.identify())) {
            helper.init((TransportClient) getClient(), settings);
            return true;
        }
        return false;
    }

    @Override
    public void closeClient(Settings settings) {
        helper.closeClient(settings);
    }
}
