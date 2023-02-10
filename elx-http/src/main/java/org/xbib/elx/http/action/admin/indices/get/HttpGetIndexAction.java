package org.xbib.elx.http.action.admin.indices.get;

import org.elasticsearch.action.admin.indices.get.GetIndexAction;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.xcontent.XContentParser;
import org.xbib.elx.http.HttpAction;
import org.xbib.net.http.client.HttpResponse;
import org.xbib.net.http.client.netty.HttpRequestBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class HttpGetIndexAction extends HttpAction<GetIndexRequest, GetIndexResponse> {

    @Override
    public GetIndexAction getActionInstance() {
        return GetIndexAction.INSTANCE;
    }

    @Override
    protected HttpRequestBuilder createHttpRequest(String url, GetIndexRequest getIndexRequest) throws IOException {
        List<String> list =  getIndexRequest.indices().length == 0 ?
                List.of("*") : Arrays.asList(getIndexRequest.indices());
        String command = "/" + String.join(",", list);
        return newGetRequest(url, command);
    }

    @Override
    protected CheckedFunction<XContentParser, GetIndexResponse, IOException> entityParser(HttpResponse httpResponse) {
        return GetIndexResponse::fromXContent;
    }
}
