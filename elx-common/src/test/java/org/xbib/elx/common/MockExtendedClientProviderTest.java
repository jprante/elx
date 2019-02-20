package org.xbib.elx.common;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertNotNull;

public class MockExtendedClientProviderTest {

    @Test
    public void testMockExtendedProvider() throws IOException {
        MockExtendedClient client = ClientBuilder.builder().provider(MockExtendedClientProvider.class).build();
        assertNotNull(client);
    }
}
