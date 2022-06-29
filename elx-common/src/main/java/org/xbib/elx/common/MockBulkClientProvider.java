package org.xbib.elx.common;

import org.xbib.elx.api.BulkClientProvider;

public class MockBulkClientProvider implements BulkClientProvider<MockBulkClient> {

    @Override
    public MockBulkClient getClient(ClassLoader classLoader) {
        return new MockBulkClient();
    }
}
