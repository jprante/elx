package org.elasticsearch.action.get;

import org.elasticsearch.action.GenericAction;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.xcontent.XContentParser;
import org.xbib.elasticsearch.client.http.HttpAction;
import org.xbib.netty.http.client.RequestBuilder;

import java.io.IOException;

/**
 */
public class HttpGetAction extends HttpAction<GetRequest, GetResponse> {

    @Override
    public GenericAction<GetRequest, GetResponse> getActionInstance() {
        return GetAction.INSTANCE;
    }

    @Override
    protected RequestBuilder createHttpRequest(String url, GetRequest request) {
        return newGetRequest(url, request.index() + "/" + request.type() + "/" + request.id());
    }

    @Override
    protected CheckedFunction<XContentParser, GetResponse, IOException> entityParser() {
        return GetResponse::fromXContent;
    }
}
