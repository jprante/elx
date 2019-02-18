package org.xbib.elx.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.junit.Test;
import org.xbib.elx.common.ClientBuilder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class ExtendedTransportClientSingleNodeTest extends NodeTestUtils {

    private static final Logger logger = LogManager.getLogger(ExtendedTransportClientSingleNodeTest.class.getSimpleName());

    @Test
    public void testSingleDocNodeClient() throws Exception {
        final ExtendedTransportClient client = ClientBuilder.builder()
                .provider(ExtendedTransportClientProvider.class)
                .put(getSettings())
                .build();
        try {
            client.newIndex("test");
            client.index("test", "test", "1", true, "{ \"name\" : \"Hello World\"}"); // single doc ingest
            client.flushIngest();
            client.waitForResponses("30s");
        } catch (InterruptedException e) {
            // ignore
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            assertEquals(1, client.getBulkMetric().getSucceeded().getCount());
            if (client.hasThrowable()) {
                logger.error("error", client.getThrowable());
            }
            assertFalse(client.hasThrowable());
            client.shutdown();
        }
    }
}
