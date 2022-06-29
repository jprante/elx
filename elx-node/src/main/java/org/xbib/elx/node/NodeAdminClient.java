package org.xbib.elx.node;

import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.settings.Settings;
import org.xbib.elx.common.AbstractAdminClient;

public class NodeAdminClient extends AbstractAdminClient {

    private final NodeClientHelper helper;

    public NodeAdminClient(ClassLoader classLoader) {
        super();
        this.helper = new NodeClientHelper();
    }

    @Override
    public ElasticsearchClient createClient(Settings settings) {
        return helper.createClient(settings, null);
    }

    @Override
    public void closeClient(Settings settings) {
        helper.closeClient(settings);
    }
}
