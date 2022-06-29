package org.xbib.elx.common;

import org.xbib.elx.api.SearchClientProvider;

public class MockSearchClientProvider implements SearchClientProvider<MockSearchClient> {

    @Override
    public MockSearchClient getClient(ClassLoader classLoader) {
        return new MockSearchClient();
    }
}
