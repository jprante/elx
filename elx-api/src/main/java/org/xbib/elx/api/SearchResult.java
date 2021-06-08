package org.xbib.elx.api;

import org.elasticsearch.search.aggregations.Aggregations;

import java.util.List;

public interface SearchResult {

    long getTotal();

    long getTook();

    List<SearchDocument> getDocuments();

    Aggregations getAggregations();
}
