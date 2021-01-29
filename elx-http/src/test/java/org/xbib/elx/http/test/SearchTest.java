package org.xbib.elx.http.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.xbib.elx.common.ClientBuilder;
import org.xbib.elx.common.Parameters;
import org.xbib.elx.http.HttpBulkClient;
import org.xbib.elx.http.HttpBulkClientProvider;
import org.xbib.elx.http.HttpSearchClient;
import org.xbib.elx.http.HttpSearchClientProvider;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

@ExtendWith(TestExtension.class)
class SearchTest {

    private static final Logger logger = LogManager.getLogger(SearchTest.class.getName());

    private static final Long ACTIONS = 100L;

    private static final Long MAX_ACTIONS_PER_REQUEST = 10L;

    private final TestExtension.Helper helper;

    SearchTest(TestExtension.Helper helper) {
        this.helper = helper;
    }

    @Test
    void testDocStream() throws Exception {
        long numactions = ACTIONS;
        final HttpBulkClient bulkClient = ClientBuilder.builder()
                .setBulkClientProvider(HttpBulkClientProvider.class)
                .put(helper.getHttpSettings())
                .put(Parameters.MAX_ACTIONS_PER_REQUEST.name(), MAX_ACTIONS_PER_REQUEST)
                .build();
        try (bulkClient) {
            bulkClient.newIndex("test");
            for (int i = 0; i < ACTIONS; i++) {
                bulkClient.index("test", null, false,
                        "{ \"name\" : \"" + helper.randomString(32) + "\"}");
            }
            bulkClient.flush();
            bulkClient.waitForResponses(30L, TimeUnit.SECONDS);
            bulkClient.refreshIndex("test");
            assertEquals(numactions, bulkClient.getSearchableDocs("test"));
            bulkClient.index("test", "0", false, "{\"name\":\"Hello\"}");
            bulkClient.flush();
            bulkClient.waitForResponses(30L, TimeUnit.SECONDS);
            bulkClient.refreshIndex("test");
            assertEquals(numactions + 1, bulkClient.getSearchableDocs("test"));
        }
        assertEquals(numactions + 1, bulkClient.getBulkController().getBulkMetric().getSucceeded().getCount());
        if (bulkClient.getBulkController().getLastBulkError() != null) {
            logger.error("error", bulkClient.getBulkController().getLastBulkError());
        }
        assertNull(bulkClient.getBulkController().getLastBulkError());
        try (HttpSearchClient searchClient = ClientBuilder.builder()
                .setSearchClientProvider(HttpSearchClientProvider.class)
                .put(helper.getHttpSettings())
                .build()) {
            Optional<GetResponse> responseOptional = searchClient.get(grb -> grb.setIndex("test").setId("0"));
            assertEquals("{\"name\":\"Hello\"}", responseOptional.get().getSourceAsString());
            Stream<SearchHit> stream = searchClient.search(qb -> qb
                            .setIndices("test")
                            .setQuery(QueryBuilders.matchAllQuery()),
                    TimeValue.timeValueMinutes(1), 10);
            long count = stream.count();
            assertEquals(numactions + 1, count);
            Stream<String> ids = searchClient.getIds(qb -> qb
                    .setIndices("test")
                    .setQuery(QueryBuilders.matchAllQuery()));
            final AtomicInteger idcount = new AtomicInteger();
            ids.forEach(id -> {
                logger.info(id);
                idcount.incrementAndGet();
            });
            assertEquals(numactions + 1, idcount.get());
            assertEquals(15, searchClient.getSearchMetric().getQueries().getCount());
            assertEquals(13, searchClient.getSearchMetric().getSucceededQueries().getCount());
            assertEquals(2, searchClient.getSearchMetric().getEmptyQueries().getCount());
        }
    }
}
