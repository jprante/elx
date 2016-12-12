package org.xbib.elasticsearch.extras.client;

import static org.elasticsearch.client.Requests.indexRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.queryStringQuery;

import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.junit.Test;
import org.xbib.elasticsearch.NodeTestBase;

import java.io.IOException;

/**
 *
 */
public class WildcardTest extends NodeTestBase {

    @Test
    public void testWildcard() throws Exception {
        Client client = client("1");
        index(client, "1", "010");
        index(client, "2", "0*0");
        // exact
        validateCount(client, queryStringQuery("010").defaultField("field"), 1);
        validateCount(client, queryStringQuery("0\\*0").defaultField("field"), 1);
        // pattern
        validateCount(client, queryStringQuery("0*0").defaultField("field"), 1); // 2?
        validateCount(client, queryStringQuery("0?0").defaultField("field"), 1); // 2?
        validateCount(client, queryStringQuery("0**0").defaultField("field"), 1); // 2?
        validateCount(client, queryStringQuery("0??0").defaultField("field"), 0);
        validateCount(client, queryStringQuery("*10").defaultField("field"), 1);
        validateCount(client, queryStringQuery("*1*").defaultField("field"), 1);
        validateCount(client, queryStringQuery("*\\*0").defaultField("field"), 0); // 1?
        validateCount(client, queryStringQuery("*\\**").defaultField("field"), 0); // 1?
    }

    private void index(Client client, String id, String fieldValue) throws IOException {
        client.index(indexRequest()
                .index("index").type("type").id(id)
                .source(jsonBuilder().startObject().field("field", fieldValue).endObject())
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE))
                .actionGet();
    }

    private long count(Client client, QueryBuilder queryBuilder) {
        return client.prepareSearch("index").setTypes("type")
                .setQuery(queryBuilder)
                .execute().actionGet().getHits().getTotalHits();
    }

    private void validateCount(Client client, QueryBuilder queryBuilder, long expectedHits) {
        final long actualHits = count(client, queryBuilder);
        if (actualHits != expectedHits) {
            throw new RuntimeException("actualHits=" + actualHits + ", expectedHits=" + expectedHits);
        }
    }
}
