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
    protected ElasticsearchClient createClient(Settings settings) throws IOException {
        return helper.createClient(settings);
    }

    @Override
    protected void closeClient() {
        if (getClient() != null) {
            TransportClient client = (TransportClient) getClient();
            client.close();
            client.threadPool().shutdown();
        }
    }

    @Override
    public void init(Settings settings) throws IOException {
        super.init(settings);
        helper.init((TransportClient) getClient(), settings);
    }
}
