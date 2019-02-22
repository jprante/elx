package org.xbib.elx.common.test;

import static org.junit.Assert.assertEquals;

import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequestBuilder;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.Test;

public class SimpleTest extends TestBase {

    @Test
    public void test() throws Exception {
        try {
            DeleteIndexRequestBuilder deleteIndexRequestBuilder =
                    new DeleteIndexRequestBuilder(client("1"), DeleteIndexAction.INSTANCE, "test");
            deleteIndexRequestBuilder.execute().actionGet();
        } catch (IndexNotFoundException e) {
            // ignore if index not found
        }
        Settings indexSettings = Settings.builder()
                .put("index.analysis.analyzer.default.filter.0", "lowercase")
                .put("index.analysis.analyzer.default.filter.1", "trim")
                .put("index.analysis.analyzer.default.tokenizer", "keyword")
                .build();
        CreateIndexRequestBuilder createIndexRequestBuilder = new CreateIndexRequestBuilder(client("1"), CreateIndexAction.INSTANCE);
        createIndexRequestBuilder.setIndex("test")
                .setSettings(indexSettings).execute().actionGet();

        IndexRequestBuilder indexRequestBuilder = new IndexRequestBuilder(client("1"), IndexAction.INSTANCE);
        indexRequestBuilder
                .setIndex("test")
                .setType("test")
                .setId("1")
                .setSource(XContentFactory.jsonBuilder().startObject().field("field",
                        "1%2fPJJP3JV2C24iDfEu9XpHBaYxXh%2fdHTbmchB35SDznXO2g8Vz4D7GTIvY54iMiX_149c95f02a8").endObject())
                .execute()
                .actionGet();
        RefreshRequestBuilder refreshRequestBuilder = new RefreshRequestBuilder(client("1"), RefreshAction.INSTANCE);
        refreshRequestBuilder.setIndices("test").execute().actionGet();
        String doc = client("1").prepareSearch("test")
                .setTypes("test")
                .setQuery(QueryBuilders.matchQuery("field",
                        "1%2fPJJP3JV2C24iDfEu9XpHBaYxXh%2fdHTbmchB35SDznXO2g8Vz4D7GTIvY54iMiX_149c95f02a8"))
                .execute()
                .actionGet()
                .getHits().getAt(0).getSourceAsString();

        assertEquals(doc,
                "{\"field\":\"1%2fPJJP3JV2C24iDfEu9XpHBaYxXh%2fdHTbmchB35SDznXO2g8Vz4D7GTIvY54iMiX_149c95f02a8\"}");
    }
}
