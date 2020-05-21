package org.xbib.elx.common;

import org.xbib.elx.api.BulkClientProvider;

public class MockBulkClientProvider implements BulkClientProvider<MockBulkClient> {

    @Override
    public MockBulkClient getClient() {
        return new MockBulkClient();
    }
}
