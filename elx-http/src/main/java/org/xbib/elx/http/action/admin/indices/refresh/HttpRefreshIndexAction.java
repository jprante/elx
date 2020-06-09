package org.xbib.elx.http.action.admin.indices.refresh;

import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.xcontent.XContentParser;
import org.xbib.elx.http.HttpAction;
import org.xbib.netty.http.client.api.Request;
import org.xbib.netty.http.common.HttpResponse;

import java.io.IOException;

public class HttpRefreshIndexAction extends HttpAction<RefreshRequest, RefreshResponse> {

    @Override
    public RefreshAction getActionInstance() {
        return RefreshAction.INSTANCE;
    }

    @Override
    protected Request.Builder createHttpRequest(String url, RefreshRequest request) {
        String index = request.indices() != null ? String.join(",", request.indices()) + "/" : "";
        return newPostRequest(url, "/" + index + "_refresh");
    }

    @Override
    protected CheckedFunction<XContentParser, RefreshResponse, IOException> entityParser(HttpResponse httpResponse) {
        return RefreshResponse::fromXContent;
    }
}
