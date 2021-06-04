package org.xbib.elx.common;

import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.xbib.elx.api.SearchDocument;
import org.xbib.elx.api.SearchResult;

import java.util.ArrayList;
import java.util.List;

public class DefaultSearchResult implements SearchResult {

    private final SearchHits searchHits;

    public DefaultSearchResult(SearchHits searchHits) {
        this.searchHits = searchHits;
    }
    @Override
    public long getTotal() {
        return searchHits.getTotalHits();
    }

    @Override
    public List<SearchDocument> getDocuments() {
        List<SearchDocument> list = new ArrayList<>();
        for (SearchHit searchHit : searchHits.getHits()) {
            list.add(new DefaultSearchDocument(searchHit));
        }
        return list;
    }
}
