package org.xbib.elx.node;

import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.settings.Settings;
import org.xbib.elx.common.AbstractSearchClient;

public class NodeSearchClient extends AbstractSearchClient {

    private final NodeClientHelper helper;

    public NodeSearchClient(ClassLoader classLoader) {
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
