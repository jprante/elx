package org.xbib.elx.http.action.index;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.xcontent.XContentParser;
import org.xbib.elx.http.HttpAction;
import org.xbib.netty.http.client.api.Request;
import org.xbib.netty.http.common.HttpResponse;

import java.io.IOException;

public class HttpIndexAction extends HttpAction<IndexRequest, IndexResponse> {

    @Override
    public ActionType<IndexResponse> getActionInstance() {
        return IndexAction.INSTANCE;
    }

    @Override
    protected Request.Builder createHttpRequest(String url, IndexRequest request) {
        String optype = request.opType() == DocWriteRequest.OpType.CREATE ? "_create" : "_doc";
        return newPutRequest(url, "/" + request.index() + "/" + optype + "/" + request.id(),
                request.source());
    }

    @Override
    protected CheckedFunction<XContentParser, IndexResponse, IOException> entityParser(HttpResponse httpResponse) {
        return IndexResponse::fromXContent;
    }
}
