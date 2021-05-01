package org.xbib.elx.api;

import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Stream;

public interface SearchClient extends BasicClient {

    boolean isSearchMetricEnabled();

    SearchMetric getSearchMetric();

    Optional<GetResponse> get(Consumer<GetRequestBuilder> getRequestBuilder);

    Optional<MultiGetResponse> multiGet(Consumer<MultiGetRequestBuilder> multiGetRequestBuilder);

    Optional<SearchResponse> search(Consumer<SearchRequestBuilder> searchRequestBuilder);

    Stream<SearchHit> search(Consumer<SearchRequestBuilder> searchRequestBuilder,
                             TimeValue scrollTime, int scrollSize);

    Stream<String> getIds(Consumer<SearchRequestBuilder> queryBuilder);
}
