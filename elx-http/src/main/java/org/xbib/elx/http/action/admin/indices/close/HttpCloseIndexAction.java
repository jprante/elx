package org.xbib.elx.http.action.admin.indices.close;

import org.elasticsearch.action.admin.indices.close.CloseIndexAction;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.xcontent.XContentParser;
import org.xbib.elx.http.HttpAction;
import org.xbib.net.http.client.HttpResponse;
import org.xbib.net.http.client.netty.HttpRequestBuilder;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class HttpCloseIndexAction extends HttpAction<CloseIndexRequest, CloseIndexResponse> {

    @Override
    public CloseIndexAction getActionInstance() {
        return CloseIndexAction.INSTANCE;
    }

    @Override
    protected HttpRequestBuilder createHttpRequest(String url, CloseIndexRequest closeIndexRequest) {
        return newPostRequest(url, "/" + String.join(",", closeIndexRequest.indices()) + "/_close");
    }

    @Override
    protected CheckedFunction<XContentParser, CloseIndexResponse, IOException> entityParser(HttpResponse httpResponse) {
        return this::fromXContent;
    }

     public CloseIndexResponse fromXContent(XContentParser parser) throws IOException {
         AcknowledgedResponse acknowledgedResponse = CloseIndexResponse.fromXContent(parser);
         if (parser.currentToken() == null) {
             parser.nextToken();
         }
         boolean shardAcknowledged = true;
         List<CloseIndexResponse.IndexResult> list = new LinkedList<>();
         return new CloseIndexResponse(acknowledgedResponse.isAcknowledged(), shardAcknowledged, list);
    }
}
