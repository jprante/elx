package org.elasticsearch.action.index;

import org.elasticsearch.action.GenericAction;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.xcontent.XContentParser;
import org.xbib.elasticsearch.client.http.HttpAction;
import org.xbib.netty.http.client.RequestBuilder;

import java.io.IOException;

/**
 */
public class HttpIndexAction extends HttpAction<IndexRequest, IndexResponse> {

    @Override
    public GenericAction<IndexRequest, IndexResponse> getActionInstance() {
        return IndexAction.INSTANCE;
    }

    @Override
    protected RequestBuilder createHttpRequest(String url, IndexRequest request) {
        return newPutRequest(url, request.index() + "/" + request.type() + "/" + request.id(),
                request.source());
    }

    @Override
    protected CheckedFunction<XContentParser, IndexResponse, IOException> entityParser() {
        return IndexResponse::fromXContent;
    }
}
