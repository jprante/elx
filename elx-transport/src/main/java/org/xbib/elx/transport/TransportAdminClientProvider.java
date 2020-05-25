package org.xbib.elx.transport;

import org.xbib.elx.api.AdminClientProvider;

public class TransportAdminClientProvider implements AdminClientProvider<TransportAdminClient> {

    @Override
    public TransportAdminClient getClient() {
        return new TransportAdminClient();
    }
}
