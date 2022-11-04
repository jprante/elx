package org.xbib.elx.http.action.search;

import org.elasticsearch.action.search.ClearScrollAction;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.xbib.elx.http.HttpAction;
import org.xbib.net.http.client.HttpResponse;
import org.xbib.net.http.client.netty.HttpRequestBuilder;

import java.io.IOException;

public class HttpClearScrollAction extends HttpAction<ClearScrollRequest, ClearScrollResponse> {

    @Override
    public ClearScrollAction getActionInstance() {
        return ClearScrollAction.INSTANCE;
    }

    @Override
    protected HttpRequestBuilder createHttpRequest(String baseUrl, ClearScrollRequest request) throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder();
        request.toXContent(builder, ToXContent.EMPTY_PARAMS);
        return newDeleteRequest(baseUrl, "_search/scroll", BytesReference.bytes(builder));
    }

    @Override
    protected CheckedFunction<XContentParser, ClearScrollResponse, IOException> entityParser(HttpResponse httpResponse) {
        return ClearScrollResponse::fromXContent;
    }
}
