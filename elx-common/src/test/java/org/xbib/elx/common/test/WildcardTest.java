package org.xbib.elx.common.test;

import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Test;

import java.io.IOException;

public class WildcardTest extends TestBase {

    @Test
    public void testWildcard() throws Exception {
        index(client("1"), "1", "010");
        index(client("1"), "2", "0*0");
        // exact
        validateCount(client("1"), QueryBuilders.queryStringQuery("010").defaultField("field"), 1);
        validateCount(client("1"), QueryBuilders.queryStringQuery("0\\*0").defaultField("field"), 1);
        // pattern
        validateCount(client("1"), QueryBuilders.queryStringQuery("0*0").defaultField("field"), 1); // 2?
        validateCount(client("1"), QueryBuilders.queryStringQuery("0?0").defaultField("field"), 1); // 2?
        validateCount(client("1"), QueryBuilders.queryStringQuery("0**0").defaultField("field"), 1); // 2?
        validateCount(client("1"), QueryBuilders.queryStringQuery("0??0").defaultField("field"), 0);
        validateCount(client("1"), QueryBuilders.queryStringQuery("*10").defaultField("field"), 1);
        validateCount(client("1"), QueryBuilders.queryStringQuery("*1*").defaultField("field"), 1);
        validateCount(client("1"), QueryBuilders.queryStringQuery("*\\*0").defaultField("field"), 0); // 1?
        validateCount(client("1"), QueryBuilders.queryStringQuery("*\\**").defaultField("field"), 0); // 1?
    }

    private void index(Client client, String id, String fieldValue) throws IOException {
        client.execute(IndexAction.INSTANCE, new IndexRequest("index", "type", id)
                .source(XContentFactory.jsonBuilder().startObject().field("field", fieldValue).endObject()))
                .actionGet();
        client.execute(RefreshAction.INSTANCE, new RefreshRequest()).actionGet();
    }

    private long count(Client client, QueryBuilder queryBuilder) {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(queryBuilder);
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("index");
        searchRequest.types("type");
        searchRequest.source(builder);
        return client.execute(SearchAction.INSTANCE, searchRequest).actionGet().getHits().getTotalHits();
    }

    private void validateCount(Client client, QueryBuilder queryBuilder, long expectedHits) {
        final long actualHits = count(client, queryBuilder);
        if (actualHits != expectedHits) {
            throw new RuntimeException("actualHits=" + actualHits + ", expectedHits=" + expectedHits);
        }
    }
}
