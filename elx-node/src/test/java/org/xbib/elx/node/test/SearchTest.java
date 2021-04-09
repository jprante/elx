package org.xbib.elx.node.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.xbib.elx.common.ClientBuilder;
import org.xbib.elx.common.Parameters;
import org.xbib.elx.node.NodeBulkClient;
import org.xbib.elx.node.NodeBulkClientProvider;
import org.xbib.elx.node.NodeSearchClient;
import org.xbib.elx.node.NodeSearchClientProvider;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

@ExtendWith(TestExtension.class)
class SearchTest {

    private static final Logger logger = LogManager.getLogger(SearchTest.class.getName());

    private static final Long ACTIONS = 100000L;

    private static final Long MAX_ACTIONS_PER_REQUEST = 100L;

    private final TestExtension.Helper helper;

    SearchTest(TestExtension.Helper helper) {
        this.helper = helper;
    }

    @Test
    void testDocStream() throws Exception {
        long numactions = ACTIONS;
        try (NodeBulkClient bulkClient = ClientBuilder.builder(helper.client())
                .setBulkClientProvider(NodeBulkClientProvider.class)
                .put(helper.getNodeSettings())
                .put(Parameters.MAX_ACTIONS_PER_REQUEST.name(), MAX_ACTIONS_PER_REQUEST)
                .build()) {
            bulkClient.newIndex("test");
            for (int i = 0; i < ACTIONS; i++) {
                bulkClient.index("test", null, false,
                        "{ \"name\" : \"" + helper.randomString(32) + "\"}");
            }
            bulkClient.flush();
            bulkClient.waitForResponses(30L, TimeUnit.SECONDS);
            bulkClient.refreshIndex("test");
            assertEquals(numactions, bulkClient.getSearchableDocs("test"));
            assertEquals(numactions, bulkClient.getBulkController().getBulkMetric().getSucceeded().getCount());
            if (bulkClient.getBulkController().getLastBulkError() != null) {
                logger.error("error", bulkClient.getBulkController().getLastBulkError());
            }
            assertNull(bulkClient.getBulkController().getLastBulkError());
        }
        try (NodeSearchClient searchClient = ClientBuilder.builder(helper.client())
                .setSearchClientProvider(NodeSearchClientProvider.class)
                .put(helper.getNodeSettings())
                .build()) {
            Stream<SearchHit> stream = searchClient.search(qb -> qb
                            .setIndices("test")
                            .setQuery(QueryBuilders.matchAllQuery()),
                    TimeValue.timeValueMillis(100), 579);
            long count = stream.count();
            assertEquals(numactions, count);
            Stream<String> ids = searchClient.getIds(qb -> qb
                    .setIndices("test")
                    .setQuery(QueryBuilders.matchAllQuery()));
            final AtomicInteger idcount = new AtomicInteger(0);
            ids.forEach(id -> idcount.incrementAndGet());
            assertEquals(numactions, idcount.get());
            assertEquals(275, searchClient.getSearchMetric().getQueries().getCount());
            assertEquals(273, searchClient.getSearchMetric().getSucceededQueries().getCount());
            assertEquals(2, searchClient.getSearchMetric().getEmptyQueries().getCount());
            assertEquals(0, searchClient.getSearchMetric().getFailedQueries().getCount());
            assertEquals(0, searchClient.getSearchMetric().getTimeoutQueries().getCount());
        }
    }
}
