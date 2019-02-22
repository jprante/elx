package org.xbib.elx.node;

import org.xbib.elx.api.ExtendedClientProvider;

public class ExtendedNodeClientProvider implements ExtendedClientProvider<ExtendedNodeClient> {
    @Override
    public ExtendedNodeClient getExtendedClient() {
        return new ExtendedNodeClient();
    }
}
