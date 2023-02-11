package org.xbib.elx.transport.test;

import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.xbib.elx.api.IndexDefinition;
import org.xbib.elx.common.ClientBuilder;
import org.xbib.elx.common.DefaultIndexDefinition;
import org.xbib.elx.transport.TransportAdminClient;
import org.xbib.elx.transport.TransportAdminClientProvider;
import org.xbib.elx.transport.TransportBulkClient;
import org.xbib.elx.transport.TransportBulkClientProvider;
import static org.junit.jupiter.api.Assertions.assertNull;

@ExtendWith(TestExtension.class)
class IndexTest {

    private static final Logger logger = LogManager.getLogger(IndexTest.class.getName());

    private final TestExtension.Helper helper;

    IndexTest(TestExtension.Helper helper) {
        this.helper = helper;
    }

    @Test
    void testIndexForceMerge() throws Exception {
        try (TransportAdminClient adminClient = ClientBuilder.builder()
                .setAdminClientProvider(TransportAdminClientProvider.class)
                .put(helper.getClientSettings())
                .build();
             TransportBulkClient bulkClient = ClientBuilder.builder()
                .setBulkClientProvider(TransportBulkClientProvider.class)
                .put(helper.getClientSettings())
                .build()) {
            IndexDefinition indexDefinition = new DefaultIndexDefinition("test", "doc");
            indexDefinition.setFullIndexName("test_force_merge");
            indexDefinition.setForceMerge(true);
            bulkClient.newIndex(indexDefinition);
            bulkClient.startBulk(indexDefinition);
            for (int i = 0; i < 1000; i++) {
                bulkClient.index(indexDefinition, helper.randomString(1), false,
                        "{ \"name\" : \"" + helper.randomString(32) + "\"}");
            }
            bulkClient.waitForResponses(30L, TimeUnit.SECONDS);
            bulkClient.stopBulk(indexDefinition);
            adminClient.forceMerge(indexDefinition, 1);
            if (bulkClient.getBulkProcessor().getLastBulkError() != null) {
                logger.error("error", bulkClient.getBulkProcessor().getLastBulkError());
            }
            assertNull(bulkClient.getBulkProcessor().getLastBulkError());
        }
    }
}
