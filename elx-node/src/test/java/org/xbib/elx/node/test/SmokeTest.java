package org.xbib.elx.node.test;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.xbib.elx.common.ClientBuilder;
import org.xbib.elx.api.IndexDefinition;
import org.xbib.elx.common.DefaultIndexDefinition;
import org.xbib.elx.node.NodeAdminClient;
import org.xbib.elx.node.NodeAdminClientProvider;
import org.xbib.elx.node.NodeBulkClient;
import org.xbib.elx.node.NodeBulkClientProvider;

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
        try (NodeAdminClient adminClient = ClientBuilder.builder(helper.client())
                .setAdminClientProvider(NodeAdminClientProvider.class)
                .put(helper.getClientSettings())
                .build();
             NodeBulkClient bulkClient = ClientBuilder.builder(helper.client())
                     .setBulkClientProvider(NodeBulkClientProvider.class)
                     .put(helper.getClientSettings())
                     .build()) {
            IndexDefinition indexDefinition =
                    new DefaultIndexDefinition(adminClient, "test_smoke", "doc", Settings.EMPTY);
            assertEquals("test_smoke", indexDefinition.getIndex());
            assertTrue(indexDefinition.getFullIndexName().startsWith("test_smoke"));
            assertEquals(0, indexDefinition.getReplicaLevel());
            assertEquals(helper.getClusterName(), adminClient.getClusterName());
            indexDefinition.setType("doc");
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
            adminClient.updateReplicaLevel(indexDefinition, 1);
            assertEquals(1, adminClient.getReplicaLevel(indexDefinition));
            assertEquals(0, bulkClient.getBulkProcessor().getBulkMetric().getFailed().getCount());
            assertEquals(6, bulkClient.getBulkProcessor().getBulkMetric().getSucceeded().getCount());
            if (bulkClient.getBulkProcessor().getLastBulkError() != null) {
                logger.error("error", bulkClient.getBulkProcessor().getLastBulkError());
            }
            assertNull(bulkClient.getBulkProcessor().getLastBulkError());
            adminClient.deleteIndex(indexDefinition);
            XContentBuilder builder = JsonXContent.contentBuilder()
                    .startObject()
                    .startObject("properties")
                    .startObject("location")
                    .field("type", "geo_point")
                    .endObject()
                    .endObject()
                    .endObject();
            indexDefinition.setMappings(Strings.toString(builder));
            bulkClient.newIndex(indexDefinition);
            assertTrue(adminClient.getMapping(indexDefinition).containsKey("properties"));
        }
    }
}
