package org.xbib.elx.http.test;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetAction;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Ignore;
import org.junit.Test;
import org.xbib.elx.common.ClientBuilder;
import org.xbib.elx.http.ExtendedHttpClient;
import org.xbib.elx.http.ExtendedHttpClientProvider;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ClientTest extends TestBase {

    private static final Logger logger = LogManager.getLogger(ClientTest.class.getName());

    @Ignore
    @Test
    public void testGet() throws Exception {
        try (ExtendedHttpClient client = ClientBuilder.builder()
                .provider(ExtendedHttpClientProvider.class)
                .put("url", "http://" + host + ":" + httpPort)
                .build()) {
            IndexRequest indexRequest = new IndexRequest();
            indexRequest.index("test");
            indexRequest.type("test");
            indexRequest.id("1");
            indexRequest.source("test", "Hello Jörg");
            IndexResponse indexResponse = client("1").execute(IndexAction.INSTANCE, indexRequest).actionGet();
            client("1").execute(RefreshAction.INSTANCE, new RefreshRequest());

            GetRequest getRequest = new GetRequest();
            getRequest.index("test");
            getRequest.type("test");
            getRequest.id("1");

            GetResponse getResponse = client.execute(GetAction.INSTANCE, getRequest).actionGet();

            assertTrue(getResponse.isExists());
            assertEquals("{\"test\":\"Hello Jörg\"}", getResponse.getSourceAsString());

        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        }
    }

    @Ignore
    @Test
    public void testMultiGet() throws Exception {
        try (ExtendedHttpClient client = ClientBuilder.builder()
                .provider(ExtendedHttpClientProvider.class)
                .put("url", "http://" + host + ":" + httpPort)
                .build()) {
            IndexRequest indexRequest = new IndexRequest();
            indexRequest.index("test");
            indexRequest.type("test");
            indexRequest.id("1");
            indexRequest.source("test", "Hello Jörg");
            IndexResponse indexResponse = client("1").execute(IndexAction.INSTANCE, indexRequest).actionGet();
            client("1").execute(RefreshAction.INSTANCE, new RefreshRequest());

            MultiGetRequest multiGetRequest = new MultiGetRequest();
            multiGetRequest.add("test", "test", "1");

            MultiGetResponse multiGetResponse = client.execute(MultiGetAction.INSTANCE, multiGetRequest).actionGet();

            assertEquals(1, multiGetResponse.getResponses().length);
            assertEquals("{\"test\":\"Hello Jörg\"}", multiGetResponse.getResponses()[0].getResponse().getSourceAsString());

        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        }
    }

    @Test
    public void testSearchDoc() throws Exception {
        try (ExtendedHttpClient client = ClientBuilder.builder()
                .provider(ExtendedHttpClientProvider.class)
                .put("url", "http://" + host + ":" + httpPort)
                .build()) {
            IndexRequest indexRequest = new IndexRequest();
            indexRequest.index("test");
            indexRequest.type("test");
            indexRequest.id("1");
            indexRequest.source("test", "Hello Jörg");
            IndexResponse indexResponse = client("1").execute(IndexAction.INSTANCE, indexRequest).actionGet();
            client("1").execute(RefreshAction.INSTANCE, new RefreshRequest());

            SearchSourceBuilder builder = new SearchSourceBuilder();
            builder.query(QueryBuilders.matchAllQuery());
            SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices("test");
            searchRequest.types("test");
            searchRequest.source(builder);
            SearchResponse searchResponse = client.execute(SearchAction.INSTANCE, searchRequest).actionGet();
            long hits = searchResponse.getHits().getTotalHits();
            assertEquals(1, hits);
            logger.info("hits = {} source = {}", hits, searchResponse.getHits().getHits()[0].getSourceAsString());
            assertEquals("{\"test\":\"Hello Jörg\"}", searchResponse.getHits().getHits()[0].getSourceAsString());
        } catch (NoNodeAvailableException e) {
            logger.warn("skipping, no node available");
        }
    }
}
