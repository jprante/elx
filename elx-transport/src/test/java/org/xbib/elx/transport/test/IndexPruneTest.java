package org.xbib.elx.transport.test;

import java.time.Instant;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.xbib.elx.api.IndexDefinition;
import org.xbib.elx.api.IndexPruneResult;
import org.xbib.elx.common.ClientBuilder;
import org.xbib.elx.common.DefaultIndexDefinition;
import org.xbib.elx.transport.TransportAdminClient;
import org.xbib.elx.transport.TransportAdminClientProvider;
import org.xbib.elx.transport.TransportBulkClient;
import org.xbib.elx.transport.TransportBulkClientProvider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(TestExtension.class)
class IndexPruneTest {

    private static final Logger logger = LogManager.getLogger(IndexShiftTest.class.getName());

    private final TestExtension.Helper helper;

    IndexPruneTest(TestExtension.Helper helper) {
        this.helper = helper;
    }

    @Test
    void testOpenClose() throws IOException {
        try (TransportAdminClient adminClient = ClientBuilder.builder()
                .setAdminClientProvider(TransportAdminClientProvider.class)
                .put(helper.getClientSettings())
                .build();
             TransportBulkClient bulkClient = ClientBuilder.builder()
                .setBulkClientProvider(TransportBulkClientProvider.class)
                .put(helper.getClientSettings())
                .build()) {
            Instant instant = Instant.now();
            IndexDefinition indexDefinition = new DefaultIndexDefinition("test", "_doc");
            indexDefinition.setIndex("test_openclose");
            indexDefinition.setFullIndexName("test_openclose");
            bulkClient.newIndex(indexDefinition);
            assertEquals(List.of("test_openclose").toString(), adminClient.allIndices().toString());
            adminClient.closeIndex(indexDefinition);
            assertEquals(List.of("test_openclose").toString(), adminClient.allClosedIndices().toString());
            assertEquals(List.of().toString(), adminClient.allClosedIndicesOlderThan(instant).toString());
            adminClient.openIndex(indexDefinition);
            assertEquals(List.of("test_openclose").toString(), adminClient.allIndices().toString());
            assertEquals(List.of().toString(), adminClient.allClosedIndices().toString());
            assertEquals(List.of().toString(), adminClient.allClosedIndicesOlderThan(instant).toString());
        }
    }

    @Test
    void testPruneWithoutClose() throws IOException {
        try (TransportAdminClient adminClient = ClientBuilder.builder()
                .setAdminClientProvider(TransportAdminClientProvider.class)
                .put(helper.getClientSettings())
                .build();
             TransportBulkClient bulkClient = ClientBuilder.builder()
                .setBulkClientProvider(TransportBulkClientProvider.class)
                .put(helper.getClientSettings())
                .build()) {
            IndexDefinition indexDefinition = new DefaultIndexDefinition("test", "doc");
            indexDefinition.setIndex("test_prune");
            indexDefinition.setFullIndexName("test_prune1");
            bulkClient.newIndex(indexDefinition);
            indexDefinition.setShift(true);
            adminClient.shiftIndex(indexDefinition, Collections.emptyList(), null);
            indexDefinition.setFullIndexName("test_prune2");
            bulkClient.newIndex(indexDefinition);
            adminClient.shiftIndex(indexDefinition, Collections.emptyList(), null);
            indexDefinition.setFullIndexName("test_prune3");
            bulkClient.newIndex(indexDefinition);
            adminClient.shiftIndex(indexDefinition, Collections.emptyList(), null);
            indexDefinition.setFullIndexName("test_prune4");
            bulkClient.newIndex(indexDefinition);
            adminClient.shiftIndex(indexDefinition, Collections.emptyList(), null);
            indexDefinition.setDelta(2);
            indexDefinition.setMinToKeep(2);
            indexDefinition.setPrune(true);
            indexDefinition.setEnabled(true);
            IndexPruneResult indexPruneResult = adminClient.pruneIndex(indexDefinition);
            logger.info("prune result = " + indexPruneResult);
            assertTrue(indexPruneResult.getDeletedIndices().contains("test_prune1"));
            assertTrue(indexPruneResult.getDeletedIndices().contains("test_prune2"));
            assertFalse(indexPruneResult.getDeletedIndices().contains("test_prune3"));
            assertFalse(indexPruneResult.getDeletedIndices().contains("test_prune4"));
            List<Boolean> list = new ArrayList<>();
            for (String index : Arrays.asList("test_prune1", "test_prune2", "test_prune3", "test_prune4")) {
                IndexDefinition indexDefinition1 = new DefaultIndexDefinition(index, null);
                indexDefinition1.setFullIndexName(index);
                list.add(adminClient.isIndexExists(indexDefinition1));
            }
            logger.info(list);
            assertFalse(list.get(0));
            assertFalse(list.get(1));
            assertTrue(list.get(2));
            assertTrue(list.get(3));
            if (bulkClient.getBulkProcessor().getLastBulkError() != null) {
                logger.error("error", bulkClient.getBulkProcessor().getLastBulkError());
            }
            assertNull(bulkClient.getBulkProcessor().getLastBulkError());
        }
    }
    @Test
    void testPruneWithClose() throws IOException {
        try (TransportAdminClient adminClient = ClientBuilder.builder()
                .setAdminClientProvider(TransportAdminClientProvider.class)
                .put(helper.getClientSettings())
                .build();
             TransportBulkClient bulkClient = ClientBuilder.builder()
                     .setBulkClientProvider(TransportBulkClientProvider.class)
                     .put(helper.getClientSettings())
                     .build()) {
            IndexDefinition indexDefinition = new DefaultIndexDefinition("test", "doc");
            indexDefinition.setIndex("test_prune");
            indexDefinition.setFullIndexName("test_prune1");
            bulkClient.newIndex(indexDefinition);
            indexDefinition.setShift(true);
            indexDefinition.setCloseShifted(true);
            adminClient.shiftIndex(indexDefinition, Collections.emptyList(), null);
            indexDefinition.setFullIndexName("test_prune2");
            bulkClient.newIndex(indexDefinition);
            adminClient.shiftIndex(indexDefinition, Collections.emptyList(), null);
            indexDefinition.setFullIndexName("test_prune3");
            bulkClient.newIndex(indexDefinition);
            adminClient.shiftIndex(indexDefinition, Collections.emptyList(), null);
            indexDefinition.setFullIndexName("test_prune4");
            bulkClient.newIndex(indexDefinition);
            adminClient.shiftIndex(indexDefinition, Collections.emptyList(), null);
            indexDefinition.setDelta(2);
            indexDefinition.setMinToKeep(2);
            indexDefinition.setPrune(true);
            indexDefinition.setEnabled(true);
            IndexPruneResult indexPruneResult = adminClient.pruneIndex(indexDefinition);
            logger.info("prune result = " + indexPruneResult);
            assertSame(indexPruneResult.getState(), IndexPruneResult.State.NONE);
            for (String index : Arrays.asList("test_prune1", "test_prune2", "test_prune3")) {
                IndexDefinition indexDefinition1 = new DefaultIndexDefinition(index, null);
                indexDefinition1.setFullIndexName(index);
                assertTrue(adminClient.isIndexExists(indexDefinition1));
                assertTrue(adminClient.isIndexClosed(indexDefinition1));
            }
            assertTrue(adminClient.isIndexOpen(indexDefinition));
            if (bulkClient.getBulkProcessor().getLastBulkError() != null) {
                logger.error("error", bulkClient.getBulkProcessor().getLastBulkError());
            }
            assertNull(bulkClient.getBulkProcessor().getLastBulkError());
        }
    }
}
