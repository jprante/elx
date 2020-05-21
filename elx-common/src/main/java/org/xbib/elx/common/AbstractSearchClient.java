package org.xbib.elx.common;

import org.elasticsearch.ElasticsearchException;
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
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.xbib.elx.api.SearchClient;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Optional;
import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public abstract class AbstractSearchClient extends AbstractNativeClient implements SearchClient {

    @Override
    public Optional<GetResponse> get(Consumer<GetRequestBuilder> getRequestBuilderConsumer) {
        GetRequestBuilder getRequestBuilder = new GetRequestBuilder(client, GetAction.INSTANCE);
        getRequestBuilderConsumer.accept(getRequestBuilder);
        GetResponse getResponse = getRequestBuilder.execute().actionGet();
        return getResponse.isExists() ? Optional.of(getResponse) : Optional.empty();
    }

    @Override
    public Optional<MultiGetResponse> multiGet(Consumer<MultiGetRequestBuilder> multiGetRequestBuilderConsumer) {
        MultiGetRequestBuilder multiGetRequestBuilder = new MultiGetRequestBuilder(client, MultiGetAction.INSTANCE);
        multiGetRequestBuilderConsumer.accept(multiGetRequestBuilder);
        MultiGetResponse multiGetItemResponse = multiGetRequestBuilder.execute().actionGet();
        return multiGetItemResponse.getResponses().length == 0 ?  Optional.empty() : Optional.of(multiGetItemResponse);
    }

    @Override
    public Optional<SearchResponse> search(Consumer<SearchRequestBuilder> queryBuilder) {
        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client, SearchAction.INSTANCE);
        queryBuilder.accept(searchRequestBuilder);
        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();
        if (searchResponse.getFailedShards() > 0) {
            StringBuilder sb = new StringBuilder("Search failed:");
            for (ShardSearchFailure failure : searchResponse.getShardFailures()) {
                sb.append("\n").append(failure.reason());
            }
            throw new ElasticsearchException(sb.toString());
        }
        return searchResponse.getHits().getHits().length == 0 ? Optional.empty() : Optional.of(searchResponse);
    }

    @Override
    public Stream<SearchHit> search(Consumer<SearchRequestBuilder> queryBuilder,
                                         TimeValue scrollTime, int scrollSize) {
        SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client, SearchAction.INSTANCE);
        queryBuilder.accept(searchRequestBuilder);
        searchRequestBuilder.setScroll(scrollTime).setSize(scrollSize);
        SearchResponse originalSearchResponse = searchRequestBuilder.execute().actionGet();
        Stream<SearchResponse> infiniteResponses = Stream.iterate(originalSearchResponse,
                searchResponse -> new SearchScrollRequestBuilder(client, SearchScrollAction.INSTANCE)
                        .setScrollId(searchResponse.getScrollId())
                        .setScroll(scrollTime)
                        .execute().actionGet());
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
