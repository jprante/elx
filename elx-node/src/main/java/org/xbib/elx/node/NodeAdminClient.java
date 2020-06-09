package org.xbib.elx.node;

import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.settings.Settings;
import org.xbib.elx.common.AbstractAdminClient;
import java.io.IOException;

public class NodeAdminClient extends AbstractAdminClient {

    private final NodeClientHelper helper;

    public NodeAdminClient() {
        this.helper = new NodeClientHelper();
    }

    @Override
    protected ElasticsearchClient createClient(Settings settings) {
        return helper.createClient(settings, null);
    }

    @Override
    protected void closeClient(Settings settings) throws IOException {
        helper.closeClient(settings);
    }
}
