package org.xbib.elx.http.test;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.xbib.elx.api.IndexDefinition;
import org.xbib.elx.common.ClientBuilder;
import org.xbib.elx.http.HttpAdminClient;
import org.xbib.elx.http.HttpAdminClientProvider;
import org.xbib.elx.http.HttpBulkClient;
import org.xbib.elx.http.HttpBulkClientProvider;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@ExtendWith(TestExtension.class)
class SmokeTest {

    private static final Logger logger = LogManager.getLogger(SmokeTest.class.getSimpleName());

    private final TestExtension.Helper helper;

    SmokeTest(TestExtension.Helper helper) {
        this.helper = helper;
    }

    @Test
    void smokeTest() throws Exception {
        final HttpAdminClient adminClient = ClientBuilder.builder()
                .setAdminClientProvider(HttpAdminClientProvider.class)
                .put(helper.getHttpSettings())
                .build();
        final HttpBulkClient bulkClient = ClientBuilder.builder()
                .setBulkClientProvider(HttpBulkClientProvider.class)
                .put(helper.getHttpSettings())
                .build();
        IndexDefinition indexDefinition = adminClient.buildIndexDefinitionFromSettings("test_smoke", Settings.builder()
                .build());
        try {
            assertEquals(helper.getClusterName(), adminClient.getClusterName());
            bulkClient.newIndex("test_smoke");
            bulkClient.index("test_smoke", "1", true, "{ \"name\" : \"Hello World\"}"); // single doc ingest
            bulkClient.flush();
            bulkClient.waitForResponses(30, TimeUnit.SECONDS);
            adminClient.checkMapping("test_smoke");
            bulkClient.update("test_smoke", "1", "{ \"name\" : \"Another name\"}");
            bulkClient.delete("test_smoke", "1");
            bulkClient.flush();
            bulkClient.waitForResponses(30, TimeUnit.SECONDS);
            adminClient.waitForRecovery("test_smoke", 10L, TimeUnit.SECONDS);
            bulkClient.delete("test_smoke", "1");
            bulkClient.index("test_smoke", "1", true, "{ \"name\" : \"Hello World\"}");
            bulkClient.flush();
            adminClient.deleteIndex("test_smoke");
            assertEquals(0, indexDefinition.getReplicaLevel());
            bulkClient.newIndex(indexDefinition);
            bulkClient.index(indexDefinition.getFullIndexName(), "1", true, "{ \"name\" : \"Hello World\"}");
            bulkClient.flush();
            bulkClient.waitForResponses(30, TimeUnit.SECONDS);
            adminClient.updateReplicaLevel(indexDefinition, 2);
            int replica = adminClient.getReplicaLevel(indexDefinition);
            assertEquals(2, replica);
        } finally {
            bulkClient.close();
            if (bulkClient.getBulkController().getLastBulkError() != null) {
                logger.error("error", bulkClient.getBulkController().getLastBulkError());
            }
            assertEquals(0, bulkClient.getBulkMetric().getFailed().getCount());
            assertEquals(6, bulkClient.getBulkMetric().getSucceeded().getCount());
            assertNull(bulkClient.getBulkController().getLastBulkError());
            // close admin after bulk
            adminClient.deleteIndex(indexDefinition);
            adminClient.close();
        }
    }
}
