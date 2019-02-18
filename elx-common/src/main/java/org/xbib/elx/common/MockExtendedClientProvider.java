package org.xbib.elx.common;

import org.xbib.elx.api.ExtendedClientProvider;

public class MockExtendedClientProvider implements ExtendedClientProvider<MockExtendedClient> {
    @Override
    public MockExtendedClient getExtendedClient() {
        return new MockExtendedClient();
    }
}
