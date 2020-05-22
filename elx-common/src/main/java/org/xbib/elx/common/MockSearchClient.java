package org.xbib.elx.common;

import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.xbib.elx.api.SearchClient;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * A mocked client, it does not perform any actions on a cluster. Useful for testing.
 */
public class MockSearchClient extends MockNativeClient implements SearchClient {

    @Override
    public ElasticsearchClient getClient() {
        return null;
    }

    @Override
    public void init(Settings settings) {
    }

    @Override
    public String getClusterName() {
        return null;
    }

    @Override
    protected ElasticsearchClient createClient(Settings settings) {
        return null;
    }

    @Override
    protected void closeClient() {
    }

    @Override
    public void close() {
        // nothing to do
    }

    @Override
    public Optional<GetResponse> get(Consumer<GetRequestBuilder> getRequestBuilder) {
        return Optional.empty();
    }

    @Override
    public Optional<MultiGetResponse> multiGet(Consumer<MultiGetRequestBuilder> multiGetRequestBuilder) {
        return Optional.empty();
    }

    @Override
    public Optional<SearchResponse> search(Consumer<SearchRequestBuilder> searchRequestBuilder) {
        return Optional.empty();
    }

    @Override
    public Stream<SearchHit> search(Consumer<SearchRequestBuilder> searchRequestBuilder, TimeValue scrollTime, int scrollSize) {
        return null;
    }

    @Override
    public Stream<String> getIds(Consumer<SearchRequestBuilder> queryBuilder) {
        return null;
    }
}
