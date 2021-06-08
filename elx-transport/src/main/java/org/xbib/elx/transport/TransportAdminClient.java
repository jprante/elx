package org.xbib.elx.transport;

import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.jboss.netty.util.Version;
import org.xbib.elx.common.AbstractAdminClient;

/**
 * Transport admin client.
 */
public class TransportAdminClient extends AbstractAdminClient {

    private final TransportClientHelper helper;

    public TransportAdminClient() {
        super();
        this.helper = new TransportClientHelper();
    }

    @Override
    public ElasticsearchClient createClient(Settings settings) {
        return helper.createClient(settings);
    }

    @Override
    public boolean init(Settings settings, String info) {
         if (super.init(settings, "Netty: " + Version.ID)) {
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
