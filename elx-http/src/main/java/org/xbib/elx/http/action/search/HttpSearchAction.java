package org.xbib.elx.http.action.search;

import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.Scroll;
import org.xbib.elx.http.HttpAction;
import org.xbib.net.http.client.HttpResponse;
import org.xbib.net.http.client.netty.HttpRequestBuilder;

import java.io.IOException;

public class HttpSearchAction extends HttpAction<SearchRequest, SearchResponse> {

    @Override
    public SearchAction getActionInstance() {
        return SearchAction.INSTANCE;
    }

    @Override
    protected HttpRequestBuilder createHttpRequest(String url, SearchRequest request) {
        Scroll scroll = request.scroll();
        String params = scroll != null ? "?scroll=" + scroll.keepAlive() : "";
        String index = request.indices() != null ? String.join(",", request.indices()) + "/" : "";
        return newPostRequest(url, index + "_search" + params, request.source().toString());
    }

    @Override
    protected CheckedFunction<XContentParser, SearchResponse, IOException> entityParser(HttpResponse httpResponse) {
        return SearchResponse::fromXContent;
    }
}
