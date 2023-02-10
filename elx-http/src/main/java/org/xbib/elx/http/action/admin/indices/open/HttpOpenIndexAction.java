package org.xbib.elx.http.action.admin.indices.open;

import org.elasticsearch.action.admin.indices.open.OpenIndexAction;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.xcontent.XContentParser;
import org.xbib.elx.http.HttpAction;
import org.xbib.net.http.client.HttpResponse;
import org.xbib.net.http.client.netty.HttpRequestBuilder;

import java.io.IOException;

public class HttpOpenIndexAction extends HttpAction<OpenIndexRequest, OpenIndexResponse> {

    @Override
    public OpenIndexAction getActionInstance() {
        return OpenIndexAction.INSTANCE;
    }

    @Override
    protected HttpRequestBuilder createHttpRequest(String url, OpenIndexRequest openIndexRequest) {
        return newPostRequest(url, "/" + String.join(",", openIndexRequest.indices()) + "/_open");
    }

    @Override
    protected CheckedFunction<XContentParser, OpenIndexResponse, IOException> entityParser(HttpResponse httpResponse) {
        return OpenIndexResponse::fromXContent;
    }
}
