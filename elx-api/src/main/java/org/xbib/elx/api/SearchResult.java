package org.xbib.elx.api;

import java.util.List;

public interface SearchResult {

    long getTotal();

    long getTook();

    List<SearchDocument> getDocuments();
}
