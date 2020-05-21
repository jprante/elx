package org.xbib.elx.common.test;

import org.junit.jupiter.api.Test;
import org.xbib.elx.common.ClientBuilder;
import org.xbib.elx.common.MockAdminClient;
import org.xbib.elx.common.MockAdminClientProvider;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class MockAdminClientProviderTest {

    @Test
    void testMockAdminProvider() throws IOException {
        MockAdminClient client = ClientBuilder.builder()
                .setAdminClientProvider(MockAdminClientProvider.class)
                .build();
        assertNotNull(client);
    }
}
