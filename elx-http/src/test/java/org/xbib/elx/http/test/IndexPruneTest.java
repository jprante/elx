package org.xbib.elx.http.test;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.xbib.elx.api.IndexDefinition;
import org.xbib.elx.api.IndexPruneResult;
import org.xbib.elx.api.IndexRetention;
import org.xbib.elx.common.ClientBuilder;
import org.xbib.elx.common.DefaultIndexDefinition;
import org.xbib.elx.common.DefaultIndexRetention;
import org.xbib.elx.http.HttpAdminClient;
import org.xbib.elx.http.HttpAdminClientProvider;
import org.xbib.elx.http.HttpBulkClient;
import org.xbib.elx.http.HttpBulkClientProvider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(TestExtension.class)
class IndexPruneTest {

    private static final Logger logger = LogManager.getLogger(IndexShiftTest.class.getName());

    private final TestExtension.Helper helper;

    IndexPruneTest(TestExtension.Helper helper) {
        this.helper = helper;
    }

    @Test
    void testPrune() throws IOException {
        final HttpAdminClient adminClient = ClientBuilder.builder()
                .setAdminClientProvider(HttpAdminClientProvider.class)
                .put(helper.getHttpSettings())
                .build();
        final HttpBulkClient bulkClient = ClientBuilder.builder()
                .setBulkClientProvider(HttpBulkClientProvider.class)
                .put(helper.getHttpSettings())
                .build();
        try {
            Settings settings = Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .build();
            bulkClient.newIndex("test1", settings);
            IndexDefinition indexDefinition = new DefaultIndexDefinition();
            indexDefinition.setIndex("test");
            indexDefinition.setFullIndexName("test1");
            adminClient.shiftIndex(indexDefinition, List.of());
            bulkClient.newIndex("test2", settings);
            indexDefinition.setFullIndexName("test2");
            adminClient.shiftIndex(indexDefinition, List.of());
            bulkClient.newIndex("test3", settings);
            indexDefinition.setFullIndexName("test3");
            adminClient.shiftIndex(indexDefinition, List.of());
            bulkClient.newIndex("test4", settings);
            indexDefinition.setFullIndexName("test4");
            adminClient.shiftIndex(indexDefinition, List.of());
            indexDefinition.setPrune(true);
            IndexRetention indexRetention = new DefaultIndexRetention();
            indexDefinition.setRetention(indexRetention);
            IndexPruneResult indexPruneResult = adminClient.pruneIndex(indexDefinition);
            logger.info(indexPruneResult.toString());
            assertTrue(indexPruneResult.getDeletedIndices().contains("test1"));
            assertTrue(indexPruneResult.getDeletedIndices().contains("test2"));
            assertFalse(indexPruneResult.getDeletedIndices().contains("test3"));
            assertFalse(indexPruneResult.getDeletedIndices().contains("test4"));
            List<Boolean> list = new ArrayList<>();
            for (String index : Arrays.asList("test1", "test2", "test3", "test4")) {
                list.add(bulkClient.isIndexExists(index));
            }
            logger.info(list);
            assertFalse(list.get(0));
            assertFalse(list.get(1));
            assertTrue(list.get(2));
            assertTrue(list.get(3));
        } finally {
            bulkClient.close();
            if (bulkClient.getBulkController().getLastBulkError() != null) {
                logger.error("error", bulkClient.getBulkController().getLastBulkError());
            }
            assertNull(bulkClient.getBulkController().getLastBulkError());
            adminClient.close();
        }
    }
}
