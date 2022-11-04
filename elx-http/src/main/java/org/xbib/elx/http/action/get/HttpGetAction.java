package org.xbib.elx.http.action.get;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.xcontent.XContentParser;
import org.xbib.elx.http.HttpAction;
import org.xbib.net.http.client.HttpResponse;
import org.xbib.net.http.client.netty.HttpRequestBuilder;

import java.io.IOException;

public class HttpGetAction extends HttpAction<GetRequest, GetResponse> {

    @Override
    public ActionType<GetResponse> getActionInstance() {
        return GetAction.INSTANCE;
    }

    @Override
    protected HttpRequestBuilder createHttpRequest(String url, GetRequest request) {
        return newGetRequest(url, "/" + request.index() + "/_doc/" + request.id());
    }

    @Override
    protected CheckedFunction<XContentParser, GetResponse, IOException> entityParser(HttpResponse httpResponse) {
        return GetResponse::fromXContent;
    }
}
