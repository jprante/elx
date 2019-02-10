package org.elasticsearch.action.search;

import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.xcontent.XContentParser;
import org.xbib.elasticsearch.client.http.HttpAction;
import org.xbib.netty.http.client.RequestBuilder;

import java.io.IOException;

/**
 *
 */
public class HttpSearchAction extends HttpAction<SearchRequest, SearchResponse> {

    @Override
    public SearchAction getActionInstance() {
        return SearchAction.INSTANCE;
    }

    @Override
    protected RequestBuilder createHttpRequest(String url, SearchRequest request) {
        String index = request.indices() != null ? "/" + String.join(",", request.indices()) : "";
        return newPostRequest(url, index + "/_search", request.source().toString() );
    }

    @Override
    protected CheckedFunction<XContentParser, SearchResponse, IOException> entityParser() {
        return parser -> {
            // TODO(jprante) build search response
            return new SearchResponse();
        };
    }
}
