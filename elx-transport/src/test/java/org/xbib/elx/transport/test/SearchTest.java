package org.xbib.elx.transport.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
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
import org.xbib.elx.transport.TransportBulkClient;
import org.xbib.elx.transport.TransportBulkClientProvider;
import org.xbib.elx.transport.TransportSearchClient;
import org.xbib.elx.transport.TransportSearchClientProvider;

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
        try (TransportBulkClient bulkClient = ClientBuilder.builder()
                .setBulkClientProvider(TransportBulkClientProvider.class)
                .put(helper.getClientSettings())
                .build()) {
            bulkClient.newIndex(indexDefinition);
            bulkClient.startBulk(indexDefinition);
            for (int i = 0; i < ACTIONS; i++) {
                bulkClient.index(indexDefinition, null, false,
                        "{ \"name\" : \"" + helper.randomString(32) + "\"}");
            }
            bulkClient.waitForResponses(30L, TimeUnit.SECONDS);
            bulkClient.refreshIndex(indexDefinition);
            assertEquals(numactions, bulkClient.getSearchableDocs(indexDefinition));
            if (bulkClient.getBulkProcessor().isBulkMetricEnabled()) {
                assertEquals(numactions, bulkClient.getBulkProcessor().getBulkMetric().getSucceeded().getCount());
            }
            if (bulkClient.getBulkProcessor().getLastBulkError() != null) {
                logger.error("error", bulkClient.getBulkProcessor().getLastBulkError());
            }
            assertNull(bulkClient.getBulkProcessor().getLastBulkError());
        }
        try (TransportSearchClient searchClient = ClientBuilder.builder()
                .setSearchClientProvider(TransportSearchClientProvider.class)
                .put(helper.getClientSettings())
                .build()) {
            // test stream count
            Stream<SearchHit> stream = searchClient.search(qb -> qb
                            .setIndices(indexDefinition.getFullIndexName())
                            .setQuery(QueryBuilders.matchAllQuery()),
                    TimeValue.timeValueMillis(100), 579);
            long count = stream.count();
            assertEquals(numactions, count);
            if (searchClient.isSearchMetricEnabled()) {
                assertEquals(0L, searchClient.getSearchMetric().getFailedQueries().getCount());
                assertEquals(0L, searchClient.getSearchMetric().getTimeoutQueries().getCount());
                assertEquals(1L, searchClient.getSearchMetric().getEmptyQueries().getCount());
            }
            // test stream docs
            stream = searchClient.search(qb -> qb
                            .setIndices(indexDefinition.getFullIndexName())
                            .setQuery(QueryBuilders.matchAllQuery()),
                    TimeValue.timeValueMillis(10), 79);
            final AtomicInteger hitcount = new AtomicInteger();
            stream.forEach(hit -> hitcount.incrementAndGet());
            assertEquals(numactions, hitcount.get());
            if (searchClient.isSearchMetricEnabled()) {
                assertEquals(0L, searchClient.getSearchMetric().getFailedQueries().getCount());
                assertEquals(0L, searchClient.getSearchMetric().getTimeoutQueries().getCount());
                assertEquals(2L, searchClient.getSearchMetric().getEmptyQueries().getCount());
            }
            // test stream doc ids
            Stream<String> ids = searchClient.getIds(qb -> qb
                    .setIndices(indexDefinition.getFullIndexName())
                    .setQuery(QueryBuilders.matchAllQuery()));
            final AtomicInteger idcount = new AtomicInteger();
            ids.forEach(id -> idcount.incrementAndGet());
            assertEquals(numactions, idcount.get());
            if (searchClient.isSearchMetricEnabled()) {
                assertEquals(1542L, searchClient.getSearchMetric().getQueries().getCount());
                assertEquals(1539L, searchClient.getSearchMetric().getSucceededQueries().getCount());
                assertEquals(3L, searchClient.getSearchMetric().getEmptyQueries().getCount());
                assertEquals(0L, searchClient.getSearchMetric().getFailedQueries().getCount());
                assertEquals(0L, searchClient.getSearchMetric().getTimeoutQueries().getCount());
            }
        }
    }
}
