package org.xbib.elx.node.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.xbib.elx.api.IndexDefinition;
import org.xbib.elx.common.ClientBuilder;
import org.xbib.elx.common.DefaultIndexDefinition;
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

    private final TestExtension.Helper helper;

    SearchTest(TestExtension.Helper helper) {
        this.helper = helper;
    }

    @Test
    void testDocStream() throws Exception {
        long numactions = ACTIONS;
        IndexDefinition indexDefinition = new DefaultIndexDefinition("test", "doc");
        try (NodeBulkClient bulkClient = ClientBuilder.builder(helper.client())
                .setBulkClientProvider(NodeBulkClientProvider.class)
                .put(helper.getClientSettings())
                .build()) {
            bulkClient.newIndex(indexDefinition);
            bulkClient.startBulk(indexDefinition);
            for (int i = 0; i < numactions; i++) {
                bulkClient.index(indexDefinition, null, false,
                        "{ \"name\" : \"" + helper.randomString(32) + "\"}");
            }
            bulkClient.stopBulk(indexDefinition);
            assertTrue(bulkClient.waitForResponses(30L, TimeUnit.SECONDS));
            bulkClient.refreshIndex(indexDefinition);
            assertEquals(numactions, bulkClient.getSearchableDocs(indexDefinition));
            assertEquals(numactions, bulkClient.getBulkProcessor().getBulkMetric().getSucceeded().getCount());
            if (bulkClient.getBulkProcessor().getLastBulkError() != null) {
                logger.error("error", bulkClient.getBulkProcessor().getLastBulkError());
            }
            assertNull(bulkClient.getBulkProcessor().getLastBulkError());
        }
        try (NodeSearchClient searchClient = ClientBuilder.builder(helper.client())
                .setSearchClientProvider(NodeSearchClientProvider.class)
                .put(helper.getClientSettings())
                .build()) {
            // test stream count
            Stream<SearchHit> stream = searchClient.search(qb -> qb
                            .setIndices(indexDefinition.getFullIndexName())
                            .setQuery(QueryBuilders.matchAllQuery()),
                    TimeValue.timeValueMillis(100), 579);
            long count = stream.count();
            assertEquals(numactions, count);
            assertEquals(0L, searchClient.getSearchMetric().getFailedQueries().getCount());
            assertEquals(0L, searchClient.getSearchMetric().getTimeoutQueries().getCount());
            assertEquals(1L, searchClient.getSearchMetric().getEmptyQueries().getCount());
            // test stream docs
            stream = searchClient.search(qb -> qb
                            .setIndices(indexDefinition.getFullIndexName())
                            .setQuery(QueryBuilders.matchAllQuery()),
                    TimeValue.timeValueMillis(10), 79);
            final AtomicInteger hitcount = new AtomicInteger();
            stream.forEach(hit -> hitcount.incrementAndGet());
            assertEquals(numactions, hitcount.get());
            assertEquals(0L, searchClient.getSearchMetric().getFailedQueries().getCount());
            assertEquals(0L, searchClient.getSearchMetric().getTimeoutQueries().getCount());
            assertEquals(2L, searchClient.getSearchMetric().getEmptyQueries().getCount());
            // test stream doc ids
            Stream<String> ids = searchClient.getIds(qb -> qb
                    .setIndices(indexDefinition.getFullIndexName())
                    .setQuery(QueryBuilders.matchAllQuery()));
            final AtomicInteger idcount = new AtomicInteger();
            ids.forEach(id -> idcount.incrementAndGet());
            assertEquals(numactions, idcount.get());
            assertEquals(1542L, searchClient.getSearchMetric().getQueries().getCount());
            assertEquals(1539L, searchClient.getSearchMetric().getSucceededQueries().getCount());
            assertEquals(3L, searchClient.getSearchMetric().getEmptyQueries().getCount());
            assertEquals(0L, searchClient.getSearchMetric().getFailedQueries().getCount());
            assertEquals(0L, searchClient.getSearchMetric().getTimeoutQueries().getCount());
        }
    }
}
