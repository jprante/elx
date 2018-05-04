package org.elasticsearch.action.main;

import org.elasticsearch.action.GenericAction;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.xcontent.XContentParser;
import org.xbib.elasticsearch.client.http.HttpAction;
import org.xbib.netty.http.client.RequestBuilder;

import java.io.IOException;

/**
 */
public class HttpMainAction extends HttpAction<MainRequest, MainResponse> {

    @Override
    public GenericAction<MainRequest, MainResponse> getActionInstance() {
        return MainAction.INSTANCE;
    }

    @Override
    protected RequestBuilder createHttpRequest(String url, MainRequest request) {
        return newGetRequest(url, "/");
    }

    @Override
    protected CheckedFunction<XContentParser, MainResponse, IOException> entityParser() {
        return MainResponse::fromXContent;
    }
}
