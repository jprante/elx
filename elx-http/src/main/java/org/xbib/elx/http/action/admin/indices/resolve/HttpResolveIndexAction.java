package org.xbib.elx.http.action.admin.indices.resolve;

import java.io.IOException;
import java.util.List;
import org.elasticsearch.action.admin.indices.resolve.ResolveIndexAction;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.xcontent.XContentParser;
import org.xbib.elx.http.HttpAction;
import org.xbib.net.http.client.HttpResponse;
import org.xbib.net.http.client.netty.HttpRequestBuilder;

/**
 * Incomplete for now.
 */
public class HttpResolveIndexAction extends HttpAction<ResolveIndexAction.Request, ResolveIndexAction.Response> {

    @Override
    public ResolveIndexAction getActionInstance() {
        return ResolveIndexAction.INSTANCE;
    }

    @Override
    protected HttpRequestBuilder createHttpRequest(String url, ResolveIndexAction.Request request) {
        String index = request.indices() != null ? String.join(",", request.indices()) + "/" : "";
        return newGetRequest(url, index + "/_resolve");
    }

    @Override
    protected CheckedFunction<XContentParser, ResolveIndexAction.Response, IOException> entityParser(HttpResponse httpResponse) {
        return this::fromXContent;
    }

    public ResolveIndexAction.Response fromXContent(XContentParser parser) throws IOException {
        if (parser.currentToken() == null) {
            parser.nextToken();
        }
        // TODO parsing
        // package private constructor. We need to build a binary stream from XContent parsing
        // to use the StreamInput constructor.
        List<ResolveIndexAction.ResolvedIndex> resolvedIndices = null;
        List<ResolveIndexAction.ResolvedAlias> resolvedAliases = null;
        List<ResolveIndexAction.ResolvedDataStream> resolvedDataStreams = null;
        return new ResolveIndexAction.Response(resolvedIndices, resolvedAliases, resolvedDataStreams);
    }
}
