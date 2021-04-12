package org.xbib.elx.transport.test;

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
import org.xbib.elx.common.Parameters;
import org.xbib.elx.transport.TransportBulkClient;
import org.xbib.elx.transport.TransportBulkClientProvider;
import org.xbib.elx.transport.TransportSearchClient;
import org.xbib.elx.transport.TransportSearchClientProvider;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@ExtendWith(TestExtension.class)
class SearchTest {

    private static final Logger logger = LogManager.getLogger(SearchTest.class.getName());

    private static final Long ACTIONS = 1000L;

    private static final Long MAX_ACTIONS_PER_REQUEST = 100L;

    private final TestExtension.Helper helper;

    SearchTest(TestExtension.Helper helper) {
        this.helper = helper;
    }

    @Test
    void testDocStream() throws Exception {
        long numactions = ACTIONS;
        try (TransportBulkClient bulkClient = ClientBuilder.builder()
                .setBulkClientProvider(TransportBulkClientProvider.class)
                .put(helper.getTransportSettings())
                .put(Parameters.MAX_ACTIONS_PER_REQUEST.name(), MAX_ACTIONS_PER_REQUEST)
                .build()) {
            IndexDefinition indexDefinition = new DefaultIndexDefinition();
            indexDefinition.setFullIndexName("test");
            indexDefinition.setType("doc");
            bulkClient.newIndex(indexDefinition);
            for (int i = 0; i < numactions; i++) {
                bulkClient.index("test", "doc", null, false,
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
        try (TransportSearchClient searchClient = ClientBuilder.builder()
                .setSearchClientProvider(TransportSearchClientProvider.class)
                .put(helper.getTransportSettings())
                .build()) {
            // test stream count
            Stream<SearchHit> stream = searchClient.search(qb -> qb
                            .setIndices("test")
                            .setQuery(QueryBuilders.matchAllQuery()),
                    TimeValue.timeValueMillis(100), 570);
            long count = stream.count();
            assertEquals(numactions, count);
            assertEquals(0L, searchClient.getSearchMetric().getFailedQueries().getCount());
            assertEquals(0L, searchClient.getSearchMetric().getTimeoutQueries().getCount());
            assertEquals(1L, searchClient.getSearchMetric().getEmptyQueries().getCount());
            // test stream docs
            stream = searchClient.search(qb -> qb
                            .setIndices("test")
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
                    .setIndices("test")
                    .setQuery(QueryBuilders.matchAllQuery()));
            final AtomicInteger idcount = new AtomicInteger();
            ids.forEach(id -> {
                idcount.incrementAndGet();
            });
            assertEquals(numactions, idcount.get());
            assertEquals(0L, searchClient.getSearchMetric().getFailedQueries().getCount());
            assertEquals(0L, searchClient.getSearchMetric().getTimeoutQueries().getCount());
            assertEquals(3L, searchClient.getSearchMetric().getEmptyQueries().getCount());
        }
    }
}
