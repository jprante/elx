package org.xbib.elasticsearch.extras.client;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.junit.Assert.assertEquals;

import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;
import org.xbib.elasticsearch.NodeTestBase;

/**
 *
 */
public class SimpleTest extends NodeTestBase {

    protected Settings getNodeSettings() {
        return Settings.builder()
                .put("cluster.name", getClusterName())
                .put("discovery.type", "local")
                .put("transport.type", "local")
                .put("http.enabled", false)
                .put("path.home", getHome())
                .put("node.max_local_storage_nodes", 5)
                .build();
    }

    @Test
    public void test() throws Exception {
        try {
            DeleteIndexRequestBuilder deleteIndexRequestBuilder =
                    new DeleteIndexRequestBuilder(client("1"), DeleteIndexAction.INSTANCE, "test");
            deleteIndexRequestBuilder.execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        CreateIndexRequestBuilder createIndexRequestBuilder = new CreateIndexRequestBuilder(client("1"),
                CreateIndexAction.INSTANCE)
                .setIndex("test")
                .setSettings(Settings.builder()
                        .put("index.analysis.analyzer.default.filter.0", "lowercase")
                        .put("index.analysis.analyzer.default.filter.1", "trim")
                        .put("index.analysis.analyzer.default.tokenizer", "keyword")
                        .build());
        createIndexRequestBuilder.execute().actionGet();

        IndexRequestBuilder indexRequestBuilder = new IndexRequestBuilder(client("1"), IndexAction.INSTANCE);
        indexRequestBuilder
                .setIndex("test")
                .setType("test")
                .setId("1")
                .setSource(jsonBuilder().startObject().field("field",
                        "1%2fPJJP3JV2C24iDfEu9XpHBaYxXh%2fdHTbmchB35SDznXO2g8Vz4D7GTIvY54iMiX_149c95f02a8").endObject())
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .execute()
                .actionGet();
        String doc = client("1").prepareSearch("test")
                .setTypes("test")
                .setQuery(matchQuery("field",
                        "1%2fPJJP3JV2C24iDfEu9XpHBaYxXh%2fdHTbmchB35SDznXO2g8Vz4D7GTIvY54iMiX_149c95f02a8"))
                .execute()
                .actionGet()
                .getHits().getAt(0).getSourceAsString();

        assertEquals(doc,
                "{\"field\":\"1%2fPJJP3JV2C24iDfEu9XpHBaYxXh%2fdHTbmchB35SDznXO2g8Vz4D7GTIvY54iMiX_149c95f02a8\"}");
    }
}
