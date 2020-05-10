package org.xbib.elx.http.action.search;

import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.xcontent.XContentParser;
import org.xbib.elx.http.HttpAction;
import org.xbib.netty.http.client.api.Request;
import org.xbib.netty.http.common.HttpResponse;

import java.io.IOException;

public class HttpSearchAction extends HttpAction<SearchRequest, SearchResponse> {

    @Override
    public SearchAction getActionInstance() {
        return SearchAction.INSTANCE;
    }

    @Override
    protected Request.Builder createHttpRequest(String url, SearchRequest request) {
        // request.indices() always empty array
        String index = request.indices() != null ? String.join(",", request.indices()) + "/" : "";
        return newPostRequest(url, index + "_search", request.source().toString());
    }

    @Override
    protected CheckedFunction<XContentParser, SearchResponse, IOException> entityParser(HttpResponse httpResponse) {
        return SearchResponse::fromXContent;
    }
}
