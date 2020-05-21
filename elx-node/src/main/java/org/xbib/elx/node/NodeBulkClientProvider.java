package org.xbib.elx.node;

import org.xbib.elx.api.BulkClientProvider;

public class NodeBulkClientProvider implements BulkClientProvider<NodeBulkClient> {

    @Override
    public NodeBulkClient getClient() {
        return new NodeBulkClient();
    }
}
