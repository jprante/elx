package org.xbib.elx.http.action.admin.cluster.node.info;

import org.elasticsearch.action.admin.cluster.node.info.NodesInfoAction;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.xcontent.XContentParser;
import org.xbib.elx.http.HttpAction;
import org.xbib.netty.http.client.api.Request;
import org.xbib.netty.http.common.HttpResponse;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 *
 */
public class HttpNodesInfoAction extends HttpAction<NodesInfoRequest, NodesInfoResponse> {

    @Override
    public NodesInfoAction getActionInstance() {
        return NodesInfoAction.INSTANCE;
    }

    /**
     * Endpoint "/_nodes/{nodeId}/{metrics}"
     *
     * @param url url
     * @param request request
     * @return HTTP request
     */
    @Override
    protected Request.Builder createHttpRequest(String url, NodesInfoRequest request) {
        StringBuilder path = new StringBuilder("/_nodes");
        if (request.nodesIds() != null) {
            String nodeIds = String.join(",", request.nodesIds());
            if (nodeIds.length() > 0) {
                path.append("/").append(nodeIds);
            }
        } else {
            path.append("/_all");
        }
        List<String> metrics = new LinkedList<>();
        if (request.http()) {
            metrics.add("http");
        }
        if (request.jvm()) {
            metrics.add("jvm");
        }
        if (request.os()) {
            metrics.add("os");
        }
        if (request.plugins()) {
            metrics.add("plugins");
        }
        if (request.process()) {
            metrics.add("process");
        }
        if (request.settings()) {
            metrics.add("settings");
        }
        if (request.threadPool()) {
            metrics.add("thread_pool");
        }
        if (request.transport()) {
            metrics.add("transport");
        }
        if (!metrics.isEmpty()) {
            path.append("/").append(String.join(",", metrics));
        }
        return newGetRequest(url, path.toString());
    }

    @Override
    protected CheckedFunction<XContentParser, NodesInfoResponse, IOException> entityParser(HttpResponse httpResponse) {
        throw new UnsupportedOperationException();
    }
}
