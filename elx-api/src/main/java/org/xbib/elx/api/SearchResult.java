package org.xbib.elx.api;

import java.util.List;

public interface SearchResult {

    long getTotal();

    List<SearchDocument> getDocuments();
}
