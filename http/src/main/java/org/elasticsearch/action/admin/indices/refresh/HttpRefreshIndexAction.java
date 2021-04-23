package org.elasticsearch.action.admin.indices.refresh;

import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.xcontent.XContentParser;
import org.xbib.elasticsearch.client.http.HttpAction;
import org.xbib.netty.http.client.RequestBuilder;

import java.io.IOException;

/**
 *
 */
public class HttpRefreshIndexAction extends HttpAction<RefreshRequest, RefreshResponse> {

    @Override
    public RefreshAction getActionInstance() {
        return RefreshAction.INSTANCE;
    }

    @Override
    protected RequestBuilder createHttpRequest(String url, RefreshRequest request) {
        String index = request.indices() != null ? "/" + String.join(",", request.indices()) : "";
        return newPostRequest(url, index + "/_refresh");
    }

    @Override
    protected CheckedFunction<XContentParser, RefreshResponse, IOException> entityParser() {
        return parser -> new RefreshResponse();
    }
}