package org.xbib.elx.common;

import org.elasticsearch.search.SearchHit;
import org.xbib.elx.api.SearchDocument;

import java.util.Map;

public class DefaultSearchDocument implements SearchDocument {

    private final SearchHit searchHit;

    public DefaultSearchDocument(SearchHit searchHit) {
        this.searchHit = searchHit;
    }

    @Override
    public String getIndex() {
        return searchHit.getIndex();
    }

    @Override
    public String getId() {
        return searchHit.getId();
    }

    @Override
    public float getScore() {
        return searchHit.getScore();
    }

    @Override
    public Map<String, Object> getFields() {
        return searchHit.sourceAsMap();
    }
}
