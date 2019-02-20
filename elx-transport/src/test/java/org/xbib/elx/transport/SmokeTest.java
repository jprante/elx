package org.xbib.elx.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.junit.Test;
import org.xbib.elx.common.ClientBuilder;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class SmokeTest extends NodeTestUtils {

    private static final Logger logger = LogManager.getLogger(SmokeTest.class.getSimpleName());

    @Test
    public void testSingleDocNodeClient() throws Exception {
        final ExtendedTransportClient client = ClientBuilder.builder()
                .provider(ExtendedTransportClientProvider.class)
                .put(getSettings())
                .build();
        try {
            client.newIndex("test");
            client.index("test", "1", true, "{ \"name\" : \"Hello World\"}"); // single doc ingest
            client.flush();
            client.waitForResponses(30, TimeUnit.SECONDS);
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            assertEquals(1, client.getBulkMetric().getSucceeded().getCount());
            client.close();
            if (client.getBulkController().getLastBulkError() != null) {
                logger.error("error", client.getBulkController().getLastBulkError());
            }
            assertNull(client.getBulkController().getLastBulkError());
        }
    }
}
