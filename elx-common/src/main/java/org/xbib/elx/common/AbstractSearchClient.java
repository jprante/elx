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
import org.elasticsearch.search.SearchHit;
import org.xbib.elx.api.SearchClient;
import org.xbib.elx.api.SearchMetric;
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

    @Override
    public SearchMetric getSearchMetric() {
        return searchMetric;
    }

    @Override
    public void init(Settings settings) throws IOException {
        super.init(settings);
        this.searchMetric = new DefaultSearchMetric();
        searchMetric.init(settings);
    }

    @Override
    public void close() throws IOException {
        super.close();
        if (searchMetric != null) {
            searchMetric.close();
        }
    }

    @Override
    public Optional<GetResponse> get(Consumer<GetRequestBuilder> getRequestBuilderConsumer) {
        GetRequestBuilder getRequestBuilder = new GetRequestBuilder(client, GetAction.INSTANCE);
        getRequestBuilderConsumer.accept(getRequestBuilder);
        ActionFuture<GetResponse> actionFuture = getRequestBuilder.execute();
        searchMetric.getCurrentQueries().inc();
        GetResponse getResponse = actionFuture.actionGet();
        searchMetric.getCurrentQueries().dec();
        searchMetric.getQueries().inc();
        searchMetric.markTotalQueries(1);
        if (getResponse.isExists()) {
            searchMetric.getSucceededQueries().inc();
        } else {
            searchMetric.getEmptyQueries().inc();
        }
        return getResponse.isExists() ? Optional.of(getResponse) : Optional.empty();
    }

    @Override
    public Optional<MultiGetResponse> multiGet(Consumer<MultiGetRequestBuilder> multiGetRequestBuilderConsumer) {
        MultiGetRequestBuilder multiGetRequestBuilder = new MultiGetRequestBuilder(client, MultiGetAction.INSTANCE);
        multiGetRequestBuilderConsumer.accept(multiGetRequestBuilder);
        ActionFuture<MultiGetResponse> actionFuture = multiGetRequestBuilder.execute();
        searchMetric.getCurrentQueries().inc();
        MultiGetResponse multiGetItemResponse = actionFuture.actionGet();
        searchMetric.getCurrentQueries().dec();
        searchMetric.getQueries().inc();
        searchMetric.markTotalQueries(1);
        boolean isempty = multiGetItemResponse.getResponses().length == 0;
        if (isempty) {
            searchMetric.getEmptyQueries().inc();
        } else {
            searchMetric.getSucceededQueries().inc();
        }
        return isempty ?  Optional.empty() : Optional.of(multiGetItemResponse);
    }

    @Override
    public Optional<SearchResponse> search(Consumer<SearchRequestBuilder> queryBuilder) {
        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client, SearchAction.INSTANCE);
        queryBuilder.accept(searchRequestBuilder);
        ActionFuture<SearchResponse> actionFuture = searchRequestBuilder.execute();
        searchMetric.getCurrentQueries().inc();
        SearchResponse searchResponse = actionFuture.actionGet();
        searchMetric.getCurrentQueries().dec();
        searchMetric.getQueries().inc();
        searchMetric.markTotalQueries(1);
        if (searchResponse.getFailedShards() > 0) {
            StringBuilder sb = new StringBuilder("Search failed:");
            for (ShardSearchFailure failure : searchResponse.getShardFailures()) {
                sb.append("\n").append(failure.reason());
            }
            searchMetric.getEmptyQueries().inc();
            throw new ElasticsearchException(sb.toString());
        }
        boolean isempty = searchResponse.getHits().getHits().length == 0;
        if (isempty) {
            searchMetric.getEmptyQueries().inc();
        } else {
            searchMetric.getSucceededQueries().inc();
        }
        return isempty ? Optional.empty() : Optional.of(searchResponse);
    }

    @Override
    public Stream<SearchHit> search(Consumer<SearchRequestBuilder> queryBuilder,
                                         TimeValue scrollTime, int scrollSize) {
        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client, SearchAction.INSTANCE);
        queryBuilder.accept(searchRequestBuilder);
        searchRequestBuilder.setScroll(scrollTime).setSize(scrollSize);
        ActionFuture<SearchResponse> actionFuture =  searchRequestBuilder.execute();
        searchMetric.getCurrentQueries().inc();
        SearchResponse originalSearchResponse = actionFuture.actionGet();
        searchMetric.getCurrentQueries().dec();
        searchMetric.getQueries().inc();
        searchMetric.markTotalQueries(1);
        boolean isempty = originalSearchResponse.getHits().getTotalHits().value == 0;
        if (isempty) {
            searchMetric.getEmptyQueries().inc();
        } else {
            searchMetric.getSucceededQueries().inc();
        }
        Stream<SearchResponse> infiniteResponses = Stream.iterate(originalSearchResponse,
                searchResponse -> {
                    SearchScrollRequestBuilder searchScrollRequestBuilder =
                            new SearchScrollRequestBuilder(client, SearchScrollAction.INSTANCE)
                            .setScrollId(searchResponse.getScrollId())
                            .setScroll(scrollTime);
                    ActionFuture<SearchResponse> actionFuture1 = searchScrollRequestBuilder.execute();
                    searchMetric.getCurrentQueries().inc();
                    SearchResponse searchResponse1 = actionFuture1.actionGet();
                    searchMetric.getCurrentQueries().dec();
                    searchMetric.getQueries().inc();
                    searchMetric.markTotalQueries(1);
                    return searchResponse1;
                });
        Predicate<SearchResponse> condition = searchResponse -> searchResponse.getHits().getHits().length > 0;
        Consumer<SearchResponse> lastAction = searchResponse -> {
            ClearScrollRequestBuilder clearScrollRequestBuilder =
                    new ClearScrollRequestBuilder(client, ClearScrollAction.INSTANCE)
                            .addScrollId(searchResponse.getScrollId());
            clearScrollRequestBuilder.execute().actionGet();
        };
        return StreamSupport.stream(TakeWhileSpliterator.over(infiniteResponses.spliterator(),
                condition, lastAction), false)
                .onClose(infiniteResponses::close)
                .flatMap(searchResponse -> Arrays.stream(searchResponse.getHits().getHits()));
    }

    @Override
    public Stream<String> getIds(Consumer<SearchRequestBuilder> queryBuilder) {
        return search(queryBuilder, TimeValue.timeValueMinutes(1), 1000).map(SearchHit::getId);
    }

    static class TakeWhileSpliterator<T> implements Spliterator<T> {

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
