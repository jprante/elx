package org.xbib.elx.transport.test;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.xbib.elx.api.IndexDefinition;
import org.xbib.elx.common.ClientBuilder;
import org.xbib.elx.common.DefaultIndexDefinition;
import org.xbib.elx.transport.TransportAdminClient;
import org.xbib.elx.transport.TransportAdminClientProvider;
import org.xbib.elx.transport.TransportBulkClient;
import org.xbib.elx.transport.TransportBulkClientProvider;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(TestExtension.class)
class SmokeTest {

    private static final Logger logger = LogManager.getLogger(SmokeTest.class.getName());

    private final TestExtension.Helper helper;

    SmokeTest(TestExtension.Helper helper) {
        this.helper = helper;
    }

    @Test
    void smokeTest() throws Exception {
        try (TransportAdminClient adminClient = ClientBuilder.builder()
                .setAdminClientProvider(TransportAdminClientProvider.class)
                .put(helper.getClientSettings())
                .build();
             TransportBulkClient bulkClient = ClientBuilder.builder()
                     .setBulkClientProvider(TransportBulkClientProvider.class)
                     .put(helper.getClientSettings())
                     .build()) {
            IndexDefinition indexDefinition =
                    new DefaultIndexDefinition(adminClient, "test", "doc", Settings.EMPTY);
            assertEquals("test", indexDefinition.getIndex());
            assertTrue(indexDefinition.getFullIndexName().startsWith("test"));
            assertEquals(helper.getClusterName(), adminClient.getClusterName());
            bulkClient.newIndex(indexDefinition);
            bulkClient.index(indexDefinition, "1", true, "{ \"name\" : \"Hello World\"}"); // single doc ingest
            assertTrue(bulkClient.waitForResponses(30, TimeUnit.SECONDS));
            adminClient.checkMapping(indexDefinition);
            bulkClient.update(indexDefinition, "1", "{ \"name\" : \"Another name\"}");
            bulkClient.delete(indexDefinition, "1");
            assertTrue(bulkClient.waitForResponses(30, TimeUnit.SECONDS));
            bulkClient.index(indexDefinition, "1", true, "{ \"name\" : \"Hello World\"}");
            bulkClient.delete(indexDefinition, "1");
            assertTrue(bulkClient.waitForResponses(30, TimeUnit.SECONDS));
            adminClient.deleteIndex(indexDefinition);
            bulkClient.newIndex(indexDefinition);
            bulkClient.index(indexDefinition, "1", true, "{ \"name\" : \"Hello World\"}");
            assertTrue(bulkClient.waitForResponses(30, TimeUnit.SECONDS));
            adminClient.updateReplicaLevel(indexDefinition);
            assertEquals(1, adminClient.getReplicaLevel(indexDefinition));
            if (bulkClient.getBulkProcessor().isBulkMetricEnabled()) {
                assertEquals(0, bulkClient.getBulkProcessor().getBulkMetric().getFailed().getCount());
                assertEquals(6, bulkClient.getBulkProcessor().getBulkMetric().getSucceeded().getCount());
            }
            if (bulkClient.getBulkProcessor().getLastBulkError() != null) {
                logger.error("error", bulkClient.getBulkProcessor().getLastBulkError());
            }
            assertNull(bulkClient.getBulkProcessor().getLastBulkError());
            adminClient.deleteIndex(indexDefinition);
            XContentBuilder builder = JsonXContent.contentBuilder()
                    .startObject()
                    .startObject("doc")
                    .startObject("properties")
                    .startObject("location")
                    .field("type", "geo_point")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject();
            indexDefinition.setMappings(builder.string());
            bulkClient.newIndex(indexDefinition);
            assertTrue(adminClient.getMapping(indexDefinition).containsKey("properties"));
        }
    }
}
