package org.xbib.elx.http.action.main;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.main.MainAction;
import org.elasticsearch.action.main.MainRequest;
import org.elasticsearch.action.main.MainResponse;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.xcontent.XContentParser;
import org.xbib.elx.http.HttpAction;
import org.xbib.netty.http.client.api.Request;
import org.xbib.netty.http.common.HttpResponse;

import java.io.IOException;

public class HttpMainAction extends HttpAction<MainRequest, MainResponse> {

    @Override
    public ActionType<MainResponse> getActionInstance() {
        return MainAction.INSTANCE;
    }

    @Override
    protected Request.Builder createHttpRequest(String url, MainRequest request) {
        return newGetRequest(url, "/");
    }

    @Override
    protected CheckedFunction<XContentParser, MainResponse, IOException> entityParser(HttpResponse httpResponse) {
        return MainResponse::fromXContent;
    }
}
