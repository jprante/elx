package org.xbib.elx.transport;

import org.xbib.elx.api.ExtendedClientProvider;

public class ExtendedTransportClientProvider implements ExtendedClientProvider<ExtendedTransportClient> {

    @Override
    public ExtendedTransportClient getExtendedClient() {
        return new ExtendedTransportClient();
    }
}
