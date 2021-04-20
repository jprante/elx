package org.xbib.elx.node;

import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.settings.Settings;
import org.xbib.elx.common.AbstractBulkClient;
import java.io.IOException;

public class NodeBulkClient extends AbstractBulkClient {

    private final NodeClientHelper helper;

    public NodeBulkClient() {
        super();
        this.helper = new NodeClientHelper();
    }

    @Override
    public ElasticsearchClient createClient(Settings settings) {
        return helper.createClient(settings, null);
    }

    @Override
    public void closeClient(Settings settings) throws IOException {
        helper.closeClient(settings);
    }
}
