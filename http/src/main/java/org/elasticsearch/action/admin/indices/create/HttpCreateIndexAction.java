package org.elasticsearch.action.admin.indices.create;

import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.xbib.elasticsearch.client.http.HttpAction;
import org.xbib.netty.http.client.Request;
import org.xbib.netty.http.client.RequestBuilder;

import java.io.IOException;

public class HttpCreateIndexAction extends HttpAction<CreateIndexRequest, CreateIndexResponse> {

    @Override
    public CreateIndexAction getActionInstance() {
        return CreateIndexAction.INSTANCE;
    }

    @Override
    protected RequestBuilder createHttpRequest(String url, CreateIndexRequest createIndexRequest) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder = createIndexRequest.toXContent(builder, ToXContent.EMPTY_PARAMS);
        return newPutRequest(url, "/" + createIndexRequest.index(), builder.bytes());
    }

    @Override
    protected CheckedFunction<XContentParser, CreateIndexResponse, IOException> entityParser() {
        return parser -> {
            // TODO(jprante) build real create index response
            return new CreateIndexResponse();
        };
    }
}
