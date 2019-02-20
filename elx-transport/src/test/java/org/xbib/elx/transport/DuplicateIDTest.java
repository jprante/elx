package org.xbib.elx.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.junit.Test;
import org.xbib.elx.common.ClientBuilder;
import org.xbib.elx.common.Parameters;

import java.util.concurrent.TimeUnit;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class DuplicateIDTest extends NodeTestUtils {

    private final static Logger logger = LogManager.getLogger(DuplicateIDTest.class.getSimpleName());

    private final static Long MAX_ACTIONS_PER_REQUEST = 1000L;

    private final static Long ACTIONS = 12345L;

    @Test
    public void testDuplicateDocIDs() throws Exception {
        long numactions = ACTIONS;
        final ExtendedTransportClient client = ClientBuilder.builder()
                .provider(ExtendedTransportClientProvider.class)
                .put(getSettings())
                .put(Parameters.MAX_ACTIONS_PER_REQUEST.name(), MAX_ACTIONS_PER_REQUEST)
                .build();
        try {
            client.newIndex("test");
            for (int i = 0; i < ACTIONS; i++) {
                client.index("test", randomString(1), false,  "{ \"name\" : \"" + randomString(32) + "\"}");
            }
            client.flush();
            client.waitForResponses(30L, TimeUnit.SECONDS);
            client.refreshIndex("test");
            SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client.getClient(), SearchAction.INSTANCE)
                    .setIndices("test")
                    .setTypes("test")
                    .setQuery(matchAllQuery());
            long hits = searchRequestBuilder.execute().actionGet().getHits().getTotalHits();
            logger.info("hits = {}", hits);
            assertTrue(hits < ACTIONS);
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        } finally {
            client.close();
            assertEquals(numactions, client.getBulkMetric().getSucceeded().getCount());
            if (client.getBulkController().getLastBulkError() != null) {
                logger.error("error", client.getBulkController().getLastBulkError());
            }
            assertNull(client.getBulkController().getLastBulkError());
        }
    }
}
