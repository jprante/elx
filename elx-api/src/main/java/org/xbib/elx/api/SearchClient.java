package org.xbib.elx.api;

import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.common.unit.TimeValue;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Stream;

public interface SearchClient extends BasicClient {

    boolean isSearchMetricEnabled();

    SearchMetric getSearchMetric();

    Optional<SearchDocument> get(Consumer<GetRequestBuilder> getRequestBuilder);

    Stream<SearchDocument> multiGet(Consumer<MultiGetRequestBuilder> multiGetRequestBuilder);

    Optional<SearchResult> search(Consumer<SearchRequestBuilder> searchRequestBuilder);

    Stream<SearchDocument> search(Consumer<SearchRequestBuilder> searchRequestBuilder,
                                  TimeValue scrollTime, int scrollSize);

    Stream<String> getIds(Consumer<SearchRequestBuilder> queryBuilder);
}
