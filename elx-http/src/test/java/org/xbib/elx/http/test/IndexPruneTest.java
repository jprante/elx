package org.xbib.elx.http.test;

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
import org.xbib.elx.http.ExtendedHttpClient;
import org.xbib.elx.http.ExtendedHttpClientProvider;

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
        final ExtendedHttpClient client = ClientBuilder.builder(helper.client("1"))
                .put(helper.getHttpSettings())
                .provider(ExtendedHttpClientProvider.class)
                .build();
        try {
            Settings settings = Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .build();
            client.newIndex("test1", settings);
            client.shiftIndex("test", "test1", Collections.emptyList());
            client.newIndex("test2", settings);
            client.shiftIndex("test", "test2", Collections.emptyList());
            client.newIndex("test3", settings);
            client.shiftIndex("test", "test3", Collections.emptyList());
            client.newIndex("test4", settings);
            client.shiftIndex("test", "test4", Collections.emptyList());
            IndexPruneResult indexPruneResult =
                    client.pruneIndex("test", "test4", 2, 2, true);
            assertTrue(indexPruneResult.getDeletedIndices().contains("test1"));
            assertTrue(indexPruneResult.getDeletedIndices().contains("test2"));
            assertFalse(indexPruneResult.getDeletedIndices().contains("test3"));
            assertFalse(indexPruneResult.getDeletedIndices().contains("test4"));
            List<Boolean> list = new ArrayList<>();
            for (String index : Arrays.asList("test1", "test2", "test3", "test4")) {
                IndicesExistsRequest indicesExistsRequest = new IndicesExistsRequest();
                indicesExistsRequest.indices(index);
                IndicesExistsResponse indicesExistsResponse =
                        client.getClient().execute(IndicesExistsAction.INSTANCE, indicesExistsRequest).actionGet();
                logger.info("indices exists response for {} is {}", index, indicesExistsResponse.isExists());
                list.add(indicesExistsResponse.isExists());
            }
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
