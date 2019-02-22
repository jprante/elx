package org.xbib.elx.common.test;

import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class SearchTest extends TestBase {

    @Test
    public void testSearch() throws Exception {
        Client client = client("1");
        BulkRequestBuilder builder = new BulkRequestBuilder(client, BulkAction.INSTANCE);
        for (int i = 0; i < 1000; i++) {
            IndexRequest indexRequest = new IndexRequest("pages", "row")
                    .source(XContentFactory.jsonBuilder()
                            .startObject()
                            .field("user1", "joerg")
                            .field("user2", "joerg")
                            .field("user3", "joerg")
                            .field("user4", "joerg")
                            .field("user5", "joerg")
                            .field("user6", "joerg")
                            .field("user7", "joerg")
                            .field("user8", "joerg")
                            .field("user9", "joerg")
                            .field("rowcount", i)
                            .field("rs", 1234)
                            .endObject()
                    );
            builder.add(indexRequest);
        }
        client.bulk(builder.request()).actionGet();
        client.admin().indices().refresh(new RefreshRequest()).actionGet();

        for (int i = 0; i < 100; i++) {
            QueryBuilder queryStringBuilder = QueryBuilders.queryStringQuery("rs:" + 1234);
            SearchRequestBuilder requestBuilder = client.prepareSearch()
                    .setIndices("pages")
                    .setTypes("row")
                    .setQuery(queryStringBuilder)
                    .addSort("rowcount", SortOrder.DESC)
                    .setFrom(i * 10).setSize(10);
            SearchResponse searchResponse = requestBuilder.execute().actionGet();
            assertTrue(searchResponse.getHits().getTotalHits() > 0);
        }
    }
}
