package org.xbib.elx.http.action.get;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.get.MultiGetAction;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.xbib.elx.http.HttpAction;
import org.xbib.net.http.client.HttpResponse;
import org.xbib.net.http.client.netty.HttpRequestBuilder;

import java.io.IOException;

public class HttpMultiGetAction extends HttpAction<MultiGetRequest, MultiGetResponse> {

    @Override
    public ActionType<MultiGetResponse> getActionInstance() {
        return MultiGetAction.INSTANCE;
    }

    @Override
    protected HttpRequestBuilder createHttpRequest(String url, MultiGetRequest request) throws IOException {
        BytesReference source = XContentHelper.toXContent(request, XContentType.JSON, false);
        return newGetRequest(url, "/_mget", source);
    }

    @Override
    protected CheckedFunction<XContentParser, MultiGetResponse, IOException> entityParser(HttpResponse httpResponse) {
        return MultiGetResponse::fromXContent;
    }
}
