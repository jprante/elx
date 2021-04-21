package org.xbib.elx.common.test;

import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(TestExtension.class)
class SimpleTest {

    private final TestExtension.Helper helper;

    SimpleTest(TestExtension.Helper helper) {
        this.helper = helper;
    }

    @Test
    void testSimple() throws Exception {
        try {
            DeleteIndexRequest deleteIndexRequest =
                    new DeleteIndexRequest().indices("test");
            helper.client().execute(DeleteIndexAction.INSTANCE, deleteIndexRequest).actionGet();
        } catch (IndexNotFoundException e) {
            // ignore if index not found
        }
        Settings indexSettings = Settings.settingsBuilder()
                .put("index.analysis.analyzer.default.filter.0", "lowercase")
                .put("index.analysis.analyzer.default.filter.1", "trim")
                .put("index.analysis.analyzer.default.tokenizer", "keyword")
                .build();
        CreateIndexRequest createIndexRequest = new CreateIndexRequest();
        createIndexRequest.index("test").settings(indexSettings);
        helper.client().execute(CreateIndexAction.INSTANCE, createIndexRequest).actionGet();
        IndexRequest indexRequest = new IndexRequest();
        indexRequest.index("test").type("test").id("1")
                .source(XContentFactory.jsonBuilder().startObject().field("field",
                        "1%2fPJJP3JV2C24iDfEu9XpHBaYxXh%2fdHTbmchB35SDznXO2g8Vz4D7GTIvY54iMiX_149c95f02a8").endObject());
        helper.client().execute(IndexAction.INSTANCE, indexRequest).actionGet();
        RefreshRequest refreshRequest = new RefreshRequest();
        refreshRequest.indices("test");
        helper.client().execute(RefreshAction.INSTANCE, refreshRequest).actionGet();
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.matchQuery("field",
                "1%2fPJJP3JV2C24iDfEu9XpHBaYxXh%2fdHTbmchB35SDznXO2g8Vz4D7GTIvY54iMiX_149c95f02a8"));
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("test").types("test");
        searchRequest.source(builder);
        String doc = helper.client().execute(SearchAction.INSTANCE, searchRequest).actionGet()
                .getHits().getAt(0).getSourceAsString();
        assertEquals(doc,
                "{\"field\":\"1%2fPJJP3JV2C24iDfEu9XpHBaYxXh%2fdHTbmchB35SDznXO2g8Vz4D7GTIvY54iMiX_149c95f02a8\"}");
    }
}
