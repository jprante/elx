package org.xbib.elx.common;

import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregations;
import org.xbib.elx.api.SearchDocument;
import org.xbib.elx.api.SearchResult;

import java.util.ArrayList;
import java.util.List;

public class DefaultSearchResult implements SearchResult {

    private final SearchHits searchHits;

    private final Aggregations aggregations;

    private final long took;

    private final boolean timedout;

    public DefaultSearchResult(SearchHits searchHits,
                               Aggregations aggregations,
                               long took,
                               boolean timedout) {
        this.searchHits = searchHits;
        this.aggregations = aggregations;
        this.took = took;
        this.timedout = timedout;
    }

    @Override
    public List<SearchDocument> getDocuments() {
        List<SearchDocument> list = new ArrayList<>();
        for (SearchHit searchHit : searchHits.getHits()) {
            list.add(new DefaultSearchDocument(searchHit));
        }
        return list;
    }

    public Aggregations getAggregations() {
        return aggregations;
    }

    @Override
    public long getTotal() {
        return searchHits.getTotalHits().value;
    }

    @Override
    public long getTook() {
        return took;
    }

    @Override
    public boolean isTimedOut() {
        return timedout;
    }
}
