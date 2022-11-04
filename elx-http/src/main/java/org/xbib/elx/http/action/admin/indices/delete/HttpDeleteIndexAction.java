package org.xbib.elx.http.action.admin.indices.delete;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.xcontent.XContentParser;
import org.xbib.elx.http.HttpAction;
import org.xbib.net.http.client.HttpResponse;
import org.xbib.net.http.client.netty.HttpRequestBuilder;

import java.io.IOException;

public class HttpDeleteIndexAction extends HttpAction<DeleteIndexRequest, AcknowledgedResponse> {

    @Override
    public DeleteIndexAction getActionInstance() {
        return DeleteIndexAction.INSTANCE;
    }

    @Override
    protected HttpRequestBuilder createHttpRequest(String url, DeleteIndexRequest deleteIndexRequest) {
        return newDeleteRequest(url, "/" + String.join(",", deleteIndexRequest.indices()));
    }

    @Override
    protected CheckedFunction<XContentParser, AcknowledgedResponse, IOException> entityParser(HttpResponse httpResponse) {
        return AcknowledgedResponse::fromXContent;
    }
}
