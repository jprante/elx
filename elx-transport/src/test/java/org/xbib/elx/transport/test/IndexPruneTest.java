package org.xbib.elx.transport.test;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.xbib.elx.api.IndexDefinition;
import org.xbib.elx.api.IndexPruneResult;
import org.xbib.elx.api.IndexRetention;
import org.xbib.elx.common.ClientBuilder;
import org.xbib.elx.common.DefaultIndexDefinition;
import org.xbib.elx.common.DefaultIndexRetention;
import org.xbib.elx.transport.TransportAdminClient;
import org.xbib.elx.transport.TransportAdminClientProvider;
import org.xbib.elx.transport.TransportBulkClient;
import org.xbib.elx.transport.TransportBulkClientProvider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
        try (TransportAdminClient adminClient = ClientBuilder.builder()
                .setAdminClientProvider(TransportAdminClientProvider.class)
                .put(helper.getTransportSettings())
                .build();
             TransportBulkClient bulkClient = ClientBuilder.builder()
                     .setBulkClientProvider(TransportBulkClientProvider.class)
                     .put(helper.getTransportSettings())
                     .build()) {
            XContentBuilder builder = JsonXContent.contentBuilder()
                    .startObject()
                    .startObject("index")
                    .field("number_of_shards", 1)
                    .field("number_of_replicas", 0)
                    .endObject()
                    .endObject();
            IndexDefinition indexDefinition = new DefaultIndexDefinition();
            indexDefinition.setIndex("test_prune");
            indexDefinition.setType("doc");
            indexDefinition.setFullIndexName("test_prune1");
            indexDefinition.setSettings(builder.string());
            bulkClient.newIndex(indexDefinition);
            indexDefinition.setShift(true);
            adminClient.shiftIndex(indexDefinition, Collections.emptyList());
            indexDefinition.setFullIndexName("test_prune2");
            bulkClient.newIndex(indexDefinition);
            indexDefinition.setShift(true);
            adminClient.shiftIndex(indexDefinition, Collections.emptyList());
            indexDefinition.setFullIndexName("test_prune3");
            bulkClient.newIndex(indexDefinition);
            indexDefinition.setShift(true);
            adminClient.shiftIndex(indexDefinition, Collections.emptyList());
            indexDefinition.setFullIndexName("test_prune4");
            bulkClient.newIndex(indexDefinition);
            indexDefinition.setShift(true);
            adminClient.shiftIndex(indexDefinition, Collections.emptyList());
            IndexRetention indexRetention = new DefaultIndexRetention();
            indexRetention.setDelta(2);
            indexRetention.setMinToKeep(2);
            indexDefinition.setIndex("test_prune");
            indexDefinition.setFullIndexName("test_prune4");
            indexDefinition.setRetention(indexRetention);
            indexDefinition.setEnabled(true);
            indexDefinition.setPrune(true);
            IndexPruneResult indexPruneResult = adminClient.pruneIndex(indexDefinition);
            logger.info("prune result = " + indexPruneResult);
            assertTrue(indexPruneResult.getDeletedIndices().contains("test_prune1"));
            assertTrue(indexPruneResult.getDeletedIndices().contains("test_prune2"));
            assertFalse(indexPruneResult.getDeletedIndices().contains("test_prune3"));
            assertFalse(indexPruneResult.getDeletedIndices().contains("test_prune4"));
            List<Boolean> list = new ArrayList<>();
            for (String index : Arrays.asList("test_prune1", "test_prune2", "test_prune3", "test_prune4")) {
                list.add(adminClient.isIndexExists(index));
            }
            logger.info(list);
            assertFalse(list.get(0));
            assertFalse(list.get(1));
            assertTrue(list.get(2));
            assertTrue(list.get(3));
            if (bulkClient.getBulkController().getLastBulkError() != null) {
                logger.error("error", bulkClient.getBulkController().getLastBulkError());
            }
            assertNull(bulkClient.getBulkController().getLastBulkError());
        }
    }
}
