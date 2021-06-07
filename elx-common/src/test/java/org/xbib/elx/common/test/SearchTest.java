package org.xbib.elx.common.test;

import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(TestExtension.class)
class SearchTest {

    private final TestExtension.Helper helper;

    SearchTest(TestExtension.Helper helper) {
        this.helper = helper;
    }

    @Test
    void testSearch() throws Exception {
        ElasticsearchClient client = helper.client();
        BulkRequestBuilder builder = new BulkRequestBuilder(client, BulkAction.INSTANCE);
        for (int i = 0; i < 1; i++) {
            IndexRequest indexRequest = new IndexRequest().index("pages")
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
                            .endObject());
            builder.add(indexRequest);
        }
        client.execute(BulkAction.INSTANCE, builder.request()).actionGet();
        client.execute(RefreshAction.INSTANCE, new RefreshRequest()).actionGet();
        for (int i = 0; i < 1; i++) {
            QueryBuilder queryStringBuilder = QueryBuilders.queryStringQuery("rs:" + 1234);
            SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client, SearchAction.INSTANCE)
                    .setIndices("pages")
                    .setQuery(queryStringBuilder)
                    .addSort("rowcount", SortOrder.DESC)
                    .setFrom(0)
                    .setSize(10);
            SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();
            assertTrue(searchResponse.getHits().getTotalHits().value > 0);
        }
    }
}
