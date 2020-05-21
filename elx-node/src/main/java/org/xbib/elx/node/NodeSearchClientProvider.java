package org.xbib.elx.node;

import org.xbib.elx.api.SearchClientProvider;

public class NodeSearchClientProvider implements SearchClientProvider<NodeSearchClient> {

    @Override
    public NodeSearchClient getClient() {
        return new NodeSearchClient();
    }
}
