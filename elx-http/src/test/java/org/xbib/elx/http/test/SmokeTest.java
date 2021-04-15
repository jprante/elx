package org.xbib.elx.http.test;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.xbib.elx.api.IndexDefinition;
import org.xbib.elx.common.ClientBuilder;
import org.xbib.elx.common.DefaultIndexDefinition;
import org.xbib.elx.http.HttpAdminClient;
import org.xbib.elx.http.HttpAdminClientProvider;
import org.xbib.elx.http.HttpBulkClient;
import org.xbib.elx.http.HttpBulkClientProvider;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(TestExtension.class)
class SmokeTest {

    private static final Logger logger = LogManager.getLogger(SmokeTest.class.getSimpleName());

    private final TestExtension.Helper helper;

    SmokeTest(TestExtension.Helper helper) {
        this.helper = helper;
    }

    @Test
    void smokeTest() throws Exception {
        try (HttpAdminClient adminClient = ClientBuilder.builder()
                .setAdminClientProvider(HttpAdminClientProvider.class)
                .put(helper.getHttpSettings())
                .build();
             HttpBulkClient bulkClient = ClientBuilder.builder()
                     .setBulkClientProvider(HttpBulkClientProvider.class)
                     .put(helper.getHttpSettings())
                     .build()) {
            IndexDefinition indexDefinition =
                    new DefaultIndexDefinition(adminClient, "test_smoke", "doc", Settings.EMPTY);
            assertEquals(0, indexDefinition.getReplicaLevel());
            assertEquals(helper.getClusterName(), adminClient.getClusterName());
            bulkClient.newIndex(indexDefinition);
            bulkClient.index(indexDefinition, "1", true, "{ \"name\" : \"Hello World\"}"); // single doc ingest
            bulkClient.waitForResponses(30, TimeUnit.SECONDS);
            adminClient.checkMapping(indexDefinition);
            bulkClient.update(indexDefinition, "1", "{ \"name\" : \"Another name\"}");
            bulkClient.delete(indexDefinition, "1");
            bulkClient.waitForResponses(30, TimeUnit.SECONDS);
            bulkClient.index(indexDefinition, "1", true, "{ \"name\" : \"Hello World\"}");
            bulkClient.delete(indexDefinition, "1");
            bulkClient.waitForResponses(30, TimeUnit.SECONDS);
            adminClient.deleteIndex(indexDefinition);
            bulkClient.newIndex(indexDefinition);
            bulkClient.index(indexDefinition, "1", true, "{ \"name\" : \"Hello World\"}");
            bulkClient.waitForResponses(30, TimeUnit.SECONDS);
            adminClient.updateReplicaLevel(indexDefinition, 2);
            int replica = adminClient.getReplicaLevel(indexDefinition);
            assertEquals(2, replica);
            assertEquals(0, bulkClient.getBulkController().getBulkMetric().getFailed().getCount());
            assertEquals(6, bulkClient.getBulkController().getBulkMetric().getSucceeded().getCount());
            if (bulkClient.getBulkController().getLastBulkError() != null) {
                logger.error("error", bulkClient.getBulkController().getLastBulkError());
            }
            assertNull(bulkClient.getBulkController().getLastBulkError());
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
