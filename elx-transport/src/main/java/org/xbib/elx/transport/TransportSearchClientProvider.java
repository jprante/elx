package org.xbib.elx.transport;

import org.xbib.elx.api.SearchClientProvider;

public class TransportSearchClientProvider implements SearchClientProvider<TransportSearchClient> {

    @Override
    public TransportSearchClient getClient() {
        return new TransportSearchClient();
    }
}
