package org.xbib.elx.http.action.get;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.xcontent.XContentParser;
import org.xbib.elx.http.HttpAction;
import org.xbib.netty.http.client.api.Request;
import org.xbib.netty.http.common.HttpResponse;

import java.io.IOException;

public class HttpExistsAction extends HttpAction<GetRequest, GetResponse> {

    @Override
    public ActionType<GetResponse> getActionInstance() {
        return GetAction.INSTANCE;
    }

    @Override
    protected Request.Builder createHttpRequest(String url, GetRequest request) {
        return newHeadRequest(url, "/" + request.index() + "/_doc/" + request.id());
    }

    @Override
    protected CheckedFunction<XContentParser, GetResponse, IOException> entityParser(HttpResponse httpResponse) {
        return GetResponse::fromXContent;
    }
}
