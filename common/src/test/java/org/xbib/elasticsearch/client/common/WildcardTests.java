package org.xbib.elasticsearch.client.common;

import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.testframework.ESSingleNodeTestCase;

import java.io.IOException;

public class WildcardTests extends ESSingleNodeTestCase {

    public void testWildcard() throws Exception {
        index("1", "010");
        index("2", "0*0");
        // exact
        validateCount(QueryBuilders.queryStringQuery("010").defaultField("field"), 1);
        validateCount(QueryBuilders.queryStringQuery("0\\*0").defaultField("field"), 1);
        // pattern
        validateCount(QueryBuilders.queryStringQuery("0*0").defaultField("field"), 1); // 2?
        validateCount(QueryBuilders.queryStringQuery("0?0").defaultField("field"), 1); // 2?
        validateCount(QueryBuilders.queryStringQuery("0**0").defaultField("field"), 1); // 2?
        validateCount(QueryBuilders.queryStringQuery("0??0").defaultField("field"), 0);
        validateCount(QueryBuilders.queryStringQuery("*10").defaultField("field"), 1);
        validateCount(QueryBuilders.queryStringQuery("*1*").defaultField("field"), 1);
        validateCount(QueryBuilders.queryStringQuery("*\\*0").defaultField("field"), 0); // 1?
        validateCount(QueryBuilders.queryStringQuery("*\\**").defaultField("field"), 0); // 1?
    }

    private void index(String id, String fieldValue) throws IOException {
        client().index(Requests.indexRequest()
                .index("index").type("type").id(id)
                .source(XContentFactory.jsonBuilder().startObject().field("field", fieldValue).endObject())
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE))
                .actionGet();
    }

    private void validateCount(QueryBuilder queryBuilder, long expectedHits) {
        final long actualHits = count(queryBuilder);
        if (actualHits != expectedHits) {
            throw new RuntimeException("actualHits=" + actualHits + ", expectedHits=" + expectedHits);
        }
    }

    private long count(QueryBuilder queryBuilder) {
        return client().prepareSearch("index").setTypes("type")
                .setQuery(queryBuilder)
                .execute().actionGet().getHits().getTotalHits();
    }
}
