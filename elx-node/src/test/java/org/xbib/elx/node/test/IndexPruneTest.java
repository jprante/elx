package org.xbib.elx.node.test;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsAction;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.settings.Settings;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.xbib.elx.api.IndexPruneResult;
import org.xbib.elx.common.ClientBuilder;
import org.xbib.elx.node.ExtendedNodeClient;
import org.xbib.elx.node.ExtendedNodeClientProvider;

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
        final ExtendedNodeClient client = ClientBuilder.builder(helper.client("1"))
                .provider(ExtendedNodeClientProvider.class)
                .build();
        try {
            Settings settings = Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .build();
            client.newIndex("test_prune1", settings);
            client.shiftIndex("test_prune", "test_prune1", Collections.emptyList());
            client.newIndex("test_prune2", settings);
            client.shiftIndex("test_prune", "test_prune2", Collections.emptyList());
            client.newIndex("test_prune3", settings);
            client.shiftIndex("test_prune", "test_prune3", Collections.emptyList());
            client.newIndex("test_prune4", settings);
            client.shiftIndex("test_prune", "test_prune4", Collections.emptyList());
            IndexPruneResult indexPruneResult =
                    client.pruneIndex("test_prune", "test_prune4", 2, 2, true);
            assertTrue(indexPruneResult.getDeletedIndices().contains("test_prune1"));
            assertTrue(indexPruneResult.getDeletedIndices().contains("test_prune2"));
            assertFalse(indexPruneResult.getDeletedIndices().contains("test_prune3"));
            assertFalse(indexPruneResult.getDeletedIndices().contains("test_prune4"));
            List<Boolean> list = new ArrayList<>();
            for (String index : Arrays.asList("test_prune1", "test_prune2", "test_prune3", "test_prune4")) {
                IndicesExistsRequest indicesExistsRequest = new IndicesExistsRequest();
                indicesExistsRequest.indices(index);
                IndicesExistsResponse indicesExistsResponse =
                        client.getClient().execute(IndicesExistsAction.INSTANCE, indicesExistsRequest).actionGet();
                list.add(indicesExistsResponse.isExists());
            }
            logger.info(list);
            assertFalse(list.get(0));
            assertFalse(list.get(1));
            assertTrue(list.get(2));
            assertTrue(list.get(3));
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            client.close();
            if (client.getBulkController().getLastBulkError() != null) {
                logger.error("error", client.getBulkController().getLastBulkError());
            }
            assertNull(client.getBulkController().getLastBulkError());
        }
    }
}
