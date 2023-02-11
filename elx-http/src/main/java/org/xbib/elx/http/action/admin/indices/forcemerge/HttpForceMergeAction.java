package org.xbib.elx.http.action.admin.indices.forcemerge;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeAction;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.xcontent.XContentParser;
import org.xbib.elx.http.HttpAction;
import org.xbib.net.http.client.HttpResponse;
import org.xbib.net.http.client.netty.HttpRequestBuilder;

public class HttpForceMergeAction extends HttpAction<ForceMergeRequest, ForceMergeResponse> {

    @Override
    public ActionType<ForceMergeResponse> getActionInstance() {
        return ForceMergeAction.INSTANCE;
    }

    @Override
    protected HttpRequestBuilder createHttpRequest(String url, ForceMergeRequest request) throws IOException {
        List<String> params = new ArrayList<>();
        if (request.maxNumSegments() > 0) {
            params.add("max_num_segments=" + request.maxNumSegments());
        }
        if (request.onlyExpungeDeletes()) {
            params.add("only_expunge_deletes=true");
        }
        if (!request.flush()) {
            params.add("flush=false");
        }
        String paramStr = "";
        if (!params.isEmpty()) {
            paramStr = "?" + String.join("&", params);
        }
        String index = request.indices() != null ? String.join(",", request.indices()) + "/" : "";
        return newPostRequest(url, "/" + index + "/_forcemerge" + paramStr);
    }

    @Override
    protected CheckedFunction<XContentParser, ForceMergeResponse, IOException> entityParser(HttpResponse httpResponse) {
        return ForceMergeResponse::fromXContent;
    }
}
