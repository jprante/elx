package org.xbib.elx.http.action.update;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.update.UpdateAction;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.xbib.elx.http.HttpAction;
import org.xbib.netty.http.client.api.Request;
import org.xbib.netty.http.common.HttpResponse;

import java.io.IOException;

public class HttpUpdateAction extends HttpAction<UpdateRequest, UpdateResponse> {

    @Override
    public ActionType<UpdateResponse> getActionInstance() {
        return UpdateAction.INSTANCE;
    }

    @Override
    protected Request.Builder createHttpRequest(String url, UpdateRequest updateRequest) {
        try {
            // The Java API allows update requests with different content types
            // set for the partial document and the upsert document. This client
            // only accepts update requests that have the same content types set
            // for both doc and upsert.
            XContentType xContentType = null;
            if (updateRequest.doc() != null) {
                xContentType = updateRequest.doc().getContentType();
            }
            if (updateRequest.upsertRequest() != null) {
                XContentType upsertContentType = updateRequest.upsertRequest().getContentType();
                if ((xContentType != null) && (xContentType != upsertContentType)) {
                    throw new IllegalStateException("update request cannot have different content types for doc [" +
                            xContentType + "]" + " and upsert [" + upsertContentType + "] documents");
                } else {
                    xContentType = upsertContentType;
                }
            }
            if (xContentType == null) {
                xContentType = Requests.INDEX_CONTENT_TYPE;
            }
            BytesReference source = XContentHelper.toXContent(updateRequest, xContentType, false);
            return newPostRequest(url,
                    "/" + updateRequest.index() + "/_doc/" + updateRequest.id() + "/_update",
                    source);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            return null;
        }
    }

    @Override
    protected CheckedFunction<XContentParser, UpdateResponse, IOException> entityParser(HttpResponse httpResponse) {
        return UpdateResponse::fromXContent;
    }
}
