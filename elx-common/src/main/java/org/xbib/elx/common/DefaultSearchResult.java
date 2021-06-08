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

    public DefaultSearchResult(SearchHits searchHits,
                               Aggregations aggregations,
                               long took) {
        this.searchHits = searchHits;
        this.aggregations = aggregations;
        this.took = took;
    }

    @Override
    public List<SearchDocument> getDocuments() {
        List<SearchDocument> list = new ArrayList<>();
        for (SearchHit searchHit : searchHits.getHits()) {
            list.add(new DefaultSearchDocument(searchHit));
        }
        return list;
    }

    @Override
    public Aggregations getAggregations() {
        return aggregations;
    }

    @Override
    public long getTotal() {
        return searchHits.getTotalHits();
    }

    @Override
    public long getTook() {
        return took;
    }
}
