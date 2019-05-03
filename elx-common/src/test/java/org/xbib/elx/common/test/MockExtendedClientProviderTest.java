package org.xbib.elx.common.test;

import org.junit.jupiter.api.Test;
import org.xbib.elx.common.ClientBuilder;
import org.xbib.elx.common.MockExtendedClient;
import org.xbib.elx.common.MockExtendedClientProvider;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class MockExtendedClientProviderTest {

    @Test
    void testMockExtendedProvider() throws IOException {
        MockExtendedClient client = ClientBuilder.builder().provider(MockExtendedClientProvider.class).build();
        assertNotNull(client);
    }
}
