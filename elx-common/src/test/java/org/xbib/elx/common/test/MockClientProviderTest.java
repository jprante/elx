package org.xbib.elx.common.test;

import org.junit.jupiter.api.Test;
import org.xbib.elx.common.ClientBuilder;
import org.xbib.elx.common.MockAdminClient;
import org.xbib.elx.common.MockAdminClientProvider;
import org.xbib.elx.common.MockBulkClient;
import org.xbib.elx.common.MockBulkClientProvider;
import org.xbib.elx.common.MockSearchClient;
import org.xbib.elx.common.MockSearchClientProvider;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class MockClientProviderTest {

    @Test
    void testMockAdminClientProvider() throws IOException {
        MockAdminClient client = ClientBuilder.builder()
                .setAdminClientProvider(MockAdminClientProvider.class)
                .build();
        assertNotNull(client);
    }

    @Test
    void testMockBulkClientProvider() throws IOException {
        MockBulkClient client = ClientBuilder.builder()
                .setBulkClientProvider(MockBulkClientProvider.class)
                .build();
        assertNotNull(client);
    }

    @Test
    void testMockSearchClientProvider() throws IOException {
        MockSearchClient client = ClientBuilder.builder()
                .setSearchClientProvider(MockSearchClientProvider.class)
                .build();
        assertNotNull(client);
    }
}
