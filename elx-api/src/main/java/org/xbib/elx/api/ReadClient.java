package org.xbib.elx.api;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;

public interface ReadClient {

    ActionFuture<GetResponse> get(GetRequest getRequest);

    void get(GetRequest request, ActionListener<GetResponse> listener);

    ActionFuture<MultiGetResponse> multiGet(MultiGetRequest request);

    void multiGet(MultiGetRequest request, ActionListener<MultiGetResponse> listener);

    ActionFuture<SearchResponse> search(SearchRequest request);

    void search(SearchRequest request, ActionListener<SearchResponse> listener);
}
