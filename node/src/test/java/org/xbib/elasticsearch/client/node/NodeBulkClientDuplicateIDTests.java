package org.xbib.elasticsearch.client.node;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.testframework.ESSingleNodeTestCase;
import org.xbib.elasticsearch.client.ClientBuilder;
import org.xbib.elasticsearch.client.SimpleBulkControl;
import org.xbib.elasticsearch.client.SimpleBulkMetric;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;

@ThreadLeakFilters(defaultFilters = true, filters = {TestRunnerThreadsFilter.class})
public class NodeBulkClientDuplicateIDTests extends ESSingleNodeTestCase {

    private static final Logger logger = LogManager.getLogger(NodeBulkClientDuplicateIDTests.class.getName());

    private static final long MAX_ACTIONS = 100L;

    private static final long NUM_ACTIONS = 12345L;

    public void testDuplicateDocIDs() throws Exception {
        final NodeBulkClient client = ClientBuilder.builder()
                //.put(ClientBuilder.MAX_CONCURRENT_REQUESTS, 2) // avoid EsRejectedExecutionException
                .put(ClientBuilder.MAX_ACTIONS_PER_REQUEST, MAX_ACTIONS)
                .setMetric(new SimpleBulkMetric())
                .setControl(new SimpleBulkControl())
                .getClient(client(), NodeBulkClient.class);
        try {
            client.newIndex("test");
            for (int i = 0; i < NUM_ACTIONS; i++) {
                client.index("test", "test", randomAlphaOfLength(1), false, "{ \"name\" : \"" + randomAlphaOfLength(32) + "\"}");
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
            if (client.hasThrowable()) {
                logger.error("error", client.getThrowable());
            }
            assertFalse(client.hasThrowable());
            logger.info("numactions = {}, submitted = {}, succeeded= {}, failed = {}", NUM_ACTIONS,
                    client.getMetric().getSubmitted().getCount(),
                    client.getMetric().getSucceeded().getCount(),
                    client.getMetric().getFailed().getCount());
            assertEquals(NUM_ACTIONS, client.getMetric().getSubmitted().getCount());
            assertEquals(NUM_ACTIONS, client.getMetric().getSucceeded().getCount());
        }
    }
}
