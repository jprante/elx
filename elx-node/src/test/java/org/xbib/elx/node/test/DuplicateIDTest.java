package org.xbib.elx.node.test;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.xbib.elx.common.ClientBuilder;
import org.xbib.elx.common.Parameters;
import org.xbib.elx.node.ExtendedNodeClient;
import org.xbib.elx.node.ExtendedNodeClientProvider;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(TestExtension.class)
class DuplicateIDTest {

    private static final Logger logger = LogManager.getLogger(DuplicateIDTest.class.getName());

    private static final Long MAX_ACTIONS_PER_REQUEST = 10L;

    private static final Long ACTIONS = 50L;

    private final TestExtension.Helper helper;

    DuplicateIDTest(TestExtension.Helper helper) {
        this.helper = helper;
    }

    @Test
    void testDuplicateDocIDs() throws Exception {
        long numactions = ACTIONS;
        final ExtendedNodeClient client = ClientBuilder.builder(helper.client("1"))
                .provider(ExtendedNodeClientProvider.class)
                .put(Parameters.MAX_ACTIONS_PER_REQUEST.name(), MAX_ACTIONS_PER_REQUEST)
                .build();
        try {
            client.newIndex("test_dup");
            for (int i = 0; i < ACTIONS; i++) {
                client.index("test_dup", helper.randomString(1), false,
                        "{ \"name\" : \"" + helper.randomString(32) + "\"}");
            }
            client.flush();
            client.waitForResponses(30L, TimeUnit.SECONDS);
            client.refreshIndex("test_dup");
            SearchSourceBuilder builder = new SearchSourceBuilder();
            builder.query(QueryBuilders.matchAllQuery());
            SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices("test_dup");
            searchRequest.source(builder);
            long hits = helper.client("1").execute(SearchAction.INSTANCE, searchRequest).actionGet().getHits().getTotalHits();
            logger.info("hits = {}", hits);
            assertTrue(hits < ACTIONS);
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            client.close();
            assertEquals(numactions, client.getBulkController().getBulkMetric().getSucceeded().getCount());
            if (client.getBulkController().getLastBulkError() != null) {
                logger.error("error", client.getBulkController().getLastBulkError());
            }
            assertNull(client.getBulkController().getLastBulkError());
        }
    }
}
