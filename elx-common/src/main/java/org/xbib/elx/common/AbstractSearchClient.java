package org.xbib.elx.common;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetAction;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.ClearScrollAction;
import org.elasticsearch.action.search.ClearScrollRequestBuilder;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollAction;
import org.elasticsearch.action.search.SearchScrollRequestBuilder;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.xbib.elx.api.SearchClient;
import org.xbib.elx.api.SearchDocument;
import org.xbib.elx.api.SearchMetric;
import org.xbib.elx.api.SearchResult;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Optional;
import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public abstract class AbstractSearchClient extends AbstractBasicClient implements SearchClient {

    private SearchMetric searchMetric;

    public AbstractSearchClient() {
        super();
    }

    @Override
    public boolean init(Settings settings, String info) throws IOException {
        if (super.init(settings, info)) {
            if (settings.getAsBoolean(Parameters.SEARCH_METRIC_ENABLED.getName(),
                    Parameters.SEARCH_METRIC_ENABLED.getBoolean())) {
                this.searchMetric = new DefaultSearchMetric(this, settings);
                searchMetric.init(settings);
            }
            return true;
        }
        return false;
    }

    @Override
    public void close() throws IOException {
        super.close();
        if (searchMetric != null && !searchMetric.isClosed()) {
            searchMetric.close();
        }
    }

    @Override
    public boolean isSearchMetricEnabled() {
        return searchMetric != null;
    }

    @Override
    public SearchMetric getSearchMetric() {
        return searchMetric;
    }

    @Override
    public Optional<SearchDocument> get(Consumer<GetRequestBuilder> getRequestBuilderConsumer) {
        GetRequestBuilder getRequestBuilder = new GetRequestBuilder(client, GetAction.INSTANCE);
        getRequestBuilderConsumer.accept(getRequestBuilder);
        ActionFuture<GetResponse> actionFuture = getRequestBuilder.execute();
        if (searchMetric != null) {
            searchMetric.getCurrentQueries().inc();
        }
        GetResponse getResponse = actionFuture.actionGet();
        if (searchMetric != null) {
            searchMetric.getCurrentQueries().dec();
            searchMetric.getQueries().inc();
            searchMetric.markTotalQueries(1);
        }
        if (getResponse.isExists()) {
            if (searchMetric != null) {
                searchMetric.getSucceededQueries().inc();
            }
        } else {
            if (searchMetric != null) {
                searchMetric.getEmptyQueries().inc();
            }
        }
        return getResponse.isExists() ? Optional.of(new GetDocument(getResponse)) : Optional.empty();
    }

    @Override
    public Stream<SearchDocument> multiGet(Consumer<MultiGetRequestBuilder> multiGetRequestBuilderConsumer) {
        MultiGetRequestBuilder multiGetRequestBuilder = new MultiGetRequestBuilder(client, MultiGetAction.INSTANCE);
        multiGetRequestBuilderConsumer.accept(multiGetRequestBuilder);
        ActionFuture<MultiGetResponse> actionFuture = multiGetRequestBuilder.execute();
        if (searchMetric != null) {
            searchMetric.getCurrentQueries().inc();
        }
        MultiGetResponse multiGetResponse = actionFuture.actionGet();
        if (searchMetric != null) {
            searchMetric.getCurrentQueries().dec();
            searchMetric.getQueries().inc();
            searchMetric.markTotalQueries(1);
        }
        boolean isempty = multiGetResponse.getResponses().length == 0;
        if (isempty) {
            if (searchMetric != null) {
                searchMetric.getEmptyQueries().inc();
            }
        } else {
            if (searchMetric != null) {
                searchMetric.getSucceededQueries().inc();
            }
        }
        return isempty ?  Stream.of() : Arrays.stream(multiGetResponse.getResponses())
                .filter(r -> !r.isFailed())
                .map(MultiGetDocument::new);
    }

    @Override
    public Optional<SearchResult> search(Consumer<SearchRequestBuilder> queryBuilder) {
        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client, SearchAction.INSTANCE);
        searchRequestBuilder.setTrackTotalHits(true);
        searchRequestBuilder.setTrackScores(true);
        queryBuilder.accept(searchRequestBuilder);
        ActionFuture<SearchResponse> actionFuture = searchRequestBuilder.execute();
        if (searchMetric != null) {
            searchMetric.getCurrentQueries().inc();
        }
        SearchResponse searchResponse = actionFuture.actionGet();
        if (searchMetric != null) {
            searchMetric.getCurrentQueries().dec();
            searchMetric.getQueries().inc();
            searchMetric.markTotalQueries(1);
        }
        if (searchResponse.getFailedShards() > 0) {
            StringBuilder sb = new StringBuilder("Search failed:");
            for (ShardSearchFailure failure : searchResponse.getShardFailures()) {
                sb.append("\n").append(failure.reason());
            }
            if (searchMetric != null) {
                searchMetric.getEmptyQueries().inc();
            }
            throw new ElasticsearchException(sb.toString());
        }
        boolean isempty = searchResponse.getHits().getHits().length == 0;
        if (isempty) {
            if (searchMetric != null) {
                searchMetric.getEmptyQueries().inc();
            }
        } else {
            if (searchMetric != null) {
                searchMetric.getSucceededQueries().inc();
            }
        }
        return isempty ?
                Optional.empty() :
                Optional.of(new DefaultSearchResult(searchResponse.getHits(),
                        searchResponse.getAggregations(),
                        searchResponse.getTook().getMillis(),
                        searchResponse.isTimedOut()));
    }

    @Override
    public Stream<SearchDocument> search(Consumer<SearchRequestBuilder> queryBuilder,
                                         TimeValue scrollTime, int scrollSize) {
        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client, SearchAction.INSTANCE);
        searchRequestBuilder.setTrackTotalHits(true);
        searchRequestBuilder.setTrackScores(true);
        queryBuilder.accept(searchRequestBuilder);
        searchRequestBuilder.setScroll(scrollTime).setSize(scrollSize);
        ActionFuture<SearchResponse> actionFuture = searchRequestBuilder.execute();
        if (searchMetric != null) {
            searchMetric.getCurrentQueries().inc();
        }
        SearchResponse initialSearchResponse = actionFuture.actionGet();
        if (searchMetric != null) {
            searchMetric.getCurrentQueries().dec();
            searchMetric.getQueries().inc();
            searchMetric.markTotalQueries(1);
        }
        if (initialSearchResponse.getFailedShards() > 0) {
            if (searchMetric != null) {
                searchMetric.getFailedQueries().inc();
            }
        } else if (initialSearchResponse.isTimedOut()) {
            if (searchMetric != null) {
                searchMetric.getTimeoutQueries().inc();
            }
        } else if (initialSearchResponse.getHits().getTotalHits().value == 0) {
            if (searchMetric != null) {
                searchMetric.getEmptyQueries().inc();
            }
        } else {
            if (searchMetric != null) {
                searchMetric.getSucceededQueries().inc();
            }
        }
        Stream<SearchResponse> responseStream = Stream.iterate(initialSearchResponse,
                searchResponse -> {
                    SearchScrollRequestBuilder searchScrollRequestBuilder =
                            new SearchScrollRequestBuilder(client, SearchScrollAction.INSTANCE)
                                    .setScrollId(searchResponse.getScrollId())
                                    .setScroll(scrollTime);
                    ActionFuture<SearchResponse> actionFuture1 = searchScrollRequestBuilder.execute();
                    if (searchMetric != null) {
                        searchMetric.getCurrentQueries().inc();
                    }
                    SearchResponse searchResponse1 = actionFuture1.actionGet();
                    if (searchMetric != null) {
                        searchMetric.getCurrentQueries().dec();
                        searchMetric.getQueries().inc();
                        searchMetric.markTotalQueries(1);
                    }
                    if (searchResponse1.getFailedShards() > 0) {
                        if (searchMetric != null) {
                            searchMetric.getFailedQueries().inc();
                        }
                    } else if (searchResponse1.isTimedOut()) {
                        if (searchMetric != null) {
                            searchMetric.getTimeoutQueries().inc();
                        }
                    } else if (searchResponse1.getHits().getHits().length == 0) {
                        if (searchMetric != null) {
                            searchMetric.getEmptyQueries().inc();
                        }
                    } else {
                        if (searchMetric != null) {
                            searchMetric.getSucceededQueries().inc();
                        }
                    }
                    return searchResponse1;
                });
        Predicate<SearchResponse> condition = searchResponse -> searchResponse.getHits().getHits().length > 0;
        Consumer<SearchResponse> lastAction = searchResponse -> {
            ClearScrollRequestBuilder clearScrollRequestBuilder =
                    new ClearScrollRequestBuilder(client, ClearScrollAction.INSTANCE)
                            .addScrollId(searchResponse.getScrollId());
            clearScrollRequestBuilder.execute().actionGet();
        };
        return StreamSupport.stream(TakeWhileSpliterator.over(responseStream.spliterator(),
                condition, lastAction), false)
                .onClose(responseStream::close)
                .flatMap(searchResponse ->
                        new DefaultSearchResult(searchResponse.getHits(),
                                searchResponse.getAggregations(),
                                searchResponse.getTook().getMillis(),
                                searchResponse.isTimedOut()).getDocuments().stream());
    }

    @Override
    public Stream<String> getIds(Consumer<SearchRequestBuilder> queryBuilder) {
        return search(queryBuilder, TimeValue.timeValueMinutes(1), 1000).map(SearchDocument::getId);
    }

    private static class TakeWhileSpliterator<T> implements Spliterator<T> {

        private final Spliterator<T> source;

        private final Predicate<T> condition;

        private final Consumer<T> lastElement;

        private final boolean inclusive;

        private final AtomicBoolean checked;

        static <T> TakeWhileSpliterator<T> over(Spliterator<T> source,
                                                Predicate<T> condition,
                                                Consumer<T> lastElement) {
            return new TakeWhileSpliterator<>(source, condition, lastElement, true);
        }

        private TakeWhileSpliterator(Spliterator<T> source,
                                     Predicate<T> condition,
                                     Consumer<T> lastElement,
                                     boolean inclusive) {
            this.source = source;
            this.condition = condition;
            this.lastElement = lastElement;
            this.inclusive = inclusive;
            this.checked = new AtomicBoolean(true);
        }

        @Override
        public boolean tryAdvance(Consumer<? super T> action) {
            return checked.get() && source.tryAdvance(e -> {
                if (condition.test(e)) {
                    action.accept(e);
                } else {
                    if (inclusive && checked.get()) {
                        action.accept(e);
                    }
                    lastElement.accept(e);
                    checked.set(false);
                }
            });
        }

        @Override
        public Spliterator<T> trySplit() {
            return null;
        }

        @Override
        public long estimateSize() {
            return checked.get() ? source.estimateSize() : 0;
        }

        @Override
        public int characteristics() {
            return source.characteristics() &~ Spliterator.SIZED;
        }

        @Override
        public Comparator<? super T> getComparator() {
            return source.getComparator();
        }
    }
}
