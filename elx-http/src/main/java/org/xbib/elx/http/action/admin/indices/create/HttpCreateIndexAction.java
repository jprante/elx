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
import org.xbib.net.http.client.HttpResponse;
import org.xbib.net.http.client.netty.HttpRequestBuilder;

import java.io.IOException;

public class HttpCreateIndexAction extends HttpAction<CreateIndexRequest, CreateIndexResponse> {

    @Override
    public CreateIndexAction getActionInstance() {
        return CreateIndexAction.INSTANCE;
    }

    @Override
    protected HttpRequestBuilder createHttpRequest(String url, CreateIndexRequest createIndexRequest) throws IOException {
        XContentBuilder builder = createIndexRequest.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS);
        return newPutRequest(url, "/" + createIndexRequest.index() + "?include_type_name=true",
                BytesReference.bytes(builder));
    }

    @Override
    protected CheckedFunction<XContentParser, CreateIndexResponse, IOException> entityParser(HttpResponse httpResponse) {
        return CreateIndexResponse::fromXContent;
    }
}
