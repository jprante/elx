package org.xbib.elx.http.action.search;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollAction;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.xbib.elx.http.HttpAction;
import org.xbib.netty.http.client.api.Request;
import org.xbib.netty.http.common.HttpResponse;
import java.io.IOException;

public class HttpSearchScrollAction extends HttpAction<SearchScrollRequest, SearchResponse> {

    @Override
    public SearchScrollAction getActionInstance() {
        return SearchScrollAction.INSTANCE;
    }

    @Override
    protected Request.Builder createHttpRequest(String baseUrl, SearchScrollRequest request) throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder();
        request.toXContent(builder, ToXContent.EMPTY_PARAMS);
        return newPostRequest(baseUrl, "_search/scroll", BytesReference.bytes(builder));
    }

    @Override
    protected CheckedFunction<XContentParser, SearchResponse, IOException> entityParser(HttpResponse httpResponse) {
        return SearchResponse::fromXContent;
    }
}
