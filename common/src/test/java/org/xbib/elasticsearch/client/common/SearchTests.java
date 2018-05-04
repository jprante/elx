package org.xbib.elasticsearch.client.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESSingleNodeTestCase;

public class SearchTests extends ESSingleNodeTestCase {

    private static final Logger logger = LogManager.getLogger(SearchTests.class.getName());

    public void testSearch() throws Exception {
        long t0 = System.currentTimeMillis();
        BulkRequestBuilder builder = new BulkRequestBuilder(client(), BulkAction.INSTANCE);
        for (int i = 0; i < 1000; i++) {
            builder.add(Requests.indexRequest()
                    .index("pages").type("row")
                    .source(XContentFactory.jsonBuilder()
                            .startObject()
                            .field("user1", "kimchy")
                            .field("user2", "kimchy")
                            .field("user3", "kimchy")
                            .field("user4", "kimchy")
                            .field("user5", "kimchy")
                            .field("user6", "kimchy")
                            .field("user7", "kimchy")
                            .field("user8", "kimchy")
                            .field("user9", "kimchy")
                            .field("rowcount", i)
                            .field("rs", 1234)
                            .endObject()));
        }
        client().bulk(builder.request()).actionGet();
        client().admin().indices().refresh(Requests.refreshRequest()).actionGet();
        long t1 = System.currentTimeMillis();
        logger.info("t1-t0 = {}", t1 - t0);
        for (int i = 0; i < 100; i++) {
            t1 = System.currentTimeMillis();
            QueryBuilder queryStringBuilder =
                    QueryBuilders.queryStringQuery("rs:" + 1234);
            SearchRequestBuilder requestBuilder = client().prepareSearch()
                    .setIndices("pages")
                    .setTypes("row")
                    .setQuery(queryStringBuilder)
                    .addSort("rowcount", SortOrder.DESC)
                    .setFrom(i * 10).setSize(10);
            SearchResponse response = requestBuilder.execute().actionGet();
            long t2 = System.currentTimeMillis();
            logger.info("t2-t1 = {}", t2 - t1);
        }
    }
}
