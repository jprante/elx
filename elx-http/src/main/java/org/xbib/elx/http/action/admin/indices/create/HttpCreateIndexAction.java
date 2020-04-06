package org.xbib.elx.http.action.admin.indices.create;

import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.xbib.elx.http.HttpAction;
import org.xbib.netty.http.client.api.Request;

import java.io.IOException;

public class HttpCreateIndexAction extends HttpAction<CreateIndexRequest, CreateIndexResponse> {

    @Override
    public CreateIndexAction getActionInstance() {
        return CreateIndexAction.INSTANCE;
    }

    @Override
    protected Request.Builder createHttpRequest(String url, CreateIndexRequest createIndexRequest) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder = createIndexRequest.toXContent(builder, ToXContent.EMPTY_PARAMS);
        return newPutRequest(url, "/" + createIndexRequest.index(), BytesReference.bytes(builder));
    }

    @Override
    protected CheckedFunction<XContentParser, CreateIndexResponse, IOException> entityParser() {
        return CreateIndexResponse::fromXContent;
    }

    @Override
    protected CreateIndexResponse emptyResponse() {
        return new CreateIndexResponse();
    }
}
