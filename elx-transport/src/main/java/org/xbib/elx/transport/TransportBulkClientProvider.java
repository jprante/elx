package org.xbib.elx.transport;

import org.xbib.elx.api.BulkClientProvider;

public class TransportBulkClientProvider implements BulkClientProvider<TransportBulkClient> {

    @Override
    public TransportBulkClient getClient(ClassLoader classLoader) {
        return new TransportBulkClient(classLoader);
    }
}
