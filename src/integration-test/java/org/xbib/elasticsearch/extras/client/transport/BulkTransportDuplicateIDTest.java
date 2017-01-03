package org.xbib.elasticsearch.extras.client.transport;

import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.junit.Test;
import org.xbib.elasticsearch.NodeTestUtils;
import org.xbib.elasticsearch.extras.client.Clients;
import org.xbib.elasticsearch.extras.client.SimpleBulkControl;
import org.xbib.elasticsearch.extras.client.SimpleBulkMetric;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.junit.Assert.*;

public class BulkTransportDuplicateIDTest extends NodeTestUtils {

    private final static ESLogger logger = ESLoggerFactory.getLogger(BulkTransportDuplicateIDTest.class.getSimpleName());

    private final static Long MAX_ACTIONS = 1000L;

    private final static Long NUM_ACTIONS = 12345L;

    @Test
    public void testDuplicateDocIDs() throws Exception {
        long numactions = NUM_ACTIONS;
        final BulkTransportClient client = Clients.builder()
                .put(getSettings())
                .put(Clients.MAX_ACTIONS_PER_REQUEST, MAX_ACTIONS)
                .setMetric(new SimpleBulkMetric())
                .setControl(new SimpleBulkControl())
                .toBulkTransportClient();
        try {
            client.newIndex("test");
            for (int i = 0; i < NUM_ACTIONS; i++) {
                client.index("test", "test", randomString(1), "{ \"name\" : \"" + randomString(32) + "\"}");
            }
            client.flushIngest();
            client.waitForResponses("30s");
            client.refreshIndex("test");
            SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client.client(), SearchAction.INSTANCE)
                    .setIndices("test")
                    .setTypes("test")
                    .setQuery(matchAllQuery());
            long hits = searchRequestBuilder.execute().actionGet().getHits().getTotalHits();
            logger.info("hits = {}", hits);
            assertTrue(hits < NUM_ACTIONS);
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            client.shutdown();
            assertEquals(numactions, client.getMetric().getSucceeded().getCount());
            if (client.hasThrowable()) {
                logger.error("error", client.getThrowable());
            }
            assertFalse(client.hasThrowable());
        }
    }
}
