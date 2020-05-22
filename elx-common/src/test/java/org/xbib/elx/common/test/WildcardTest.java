package org.xbib.elx.common.test;

import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;

@ExtendWith(TestExtension.class)
class WildcardTest {

    private final TestExtension.Helper helper;

    WildcardTest(TestExtension.Helper helper) {
        this.helper = helper;
    }

    @Test
    void testWildcard() throws Exception {
        ElasticsearchClient client = helper.client("1");
        index(client, "1", "010");
        index(client, "2", "0*0");
        // exact
        validateCount(client, QueryBuilders.queryStringQuery("010").defaultField("field"), 1);
        validateCount(client, QueryBuilders.queryStringQuery("0\\*0").defaultField("field"), 1);
        // pattern
        validateCount(client, QueryBuilders.queryStringQuery("0*0").defaultField("field"), 1); // 2?
        validateCount(client, QueryBuilders.queryStringQuery("0?0").defaultField("field"), 1); // 2?
        validateCount(client, QueryBuilders.queryStringQuery("0**0").defaultField("field"), 1); // 2?
        validateCount(client, QueryBuilders.queryStringQuery("0??0").defaultField("field"), 0);
        validateCount(client, QueryBuilders.queryStringQuery("*10").defaultField("field"), 1);
        validateCount(client, QueryBuilders.queryStringQuery("*1*").defaultField("field"), 1);
        validateCount(client, QueryBuilders.queryStringQuery("*\\*0").defaultField("field"), 0); // 1?
        validateCount(client, QueryBuilders.queryStringQuery("*\\**").defaultField("field"), 0); // 1?
    }

    private void index(ElasticsearchClient client, String id, String fieldValue) throws IOException {
        client.execute(IndexAction.INSTANCE, new IndexRequest().index("index").type("type").id(id)
                .source(XContentFactory.jsonBuilder().startObject().field("field", fieldValue).endObject()))
                .actionGet();
        client.execute(RefreshAction.INSTANCE, new RefreshRequest()).actionGet();
    }

    private long count(ElasticsearchClient client, QueryBuilder queryBuilder) {
        SearchSourceBuilder builder = new SearchSourceBuilder()
                .query(queryBuilder);
        SearchRequest searchRequest = new SearchRequest()
                .indices("index")
                .types("type")
                .source(builder);
        return client.execute(SearchAction.INSTANCE, searchRequest).actionGet().getHits().getTotalHits();
    }

    private void validateCount(ElasticsearchClient client, QueryBuilder queryBuilder, long expectedHits) {
        final long actualHits = count(client, queryBuilder);
        if (actualHits != expectedHits) {
            throw new RuntimeException("actualHits=" + actualHits + ", expectedHits=" + expectedHits);
        }
    }
}
