package org.xbib.elx.http.action.admin.cluster.node.info;

import org.elasticsearch.action.admin.cluster.node.info.NodesInfoAction;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.xcontent.XContentParser;
import org.xbib.elx.http.HttpAction;
import org.xbib.net.http.client.HttpResponse;
import org.xbib.net.http.client.netty.HttpRequestBuilder;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class HttpNodesInfoAction extends HttpAction<NodesInfoRequest, NodesInfoResponse> {

    @Override
    public NodesInfoAction getActionInstance() {
        return NodesInfoAction.INSTANCE;
    }

    /**
     * Endpoint "/_nodes/{nodeId}/{metrics}"
     *
     * @param url     url
     * @param request request
     * @return HTTP request
     */
    @Override
    protected HttpRequestBuilder createHttpRequest(String url, NodesInfoRequest request) {
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
        if (request.requestedMetrics().contains(NodesInfoRequest.Metric.HTTP.metricName())) {
            metrics.add("http");
        }
        if (request.requestedMetrics().contains(NodesInfoRequest.Metric.JVM.metricName())) {
            metrics.add("jvm");
        }
        if (request.requestedMetrics().contains(NodesInfoRequest.Metric.OS.metricName())) {
            metrics.add("os");
        }
        if (request.requestedMetrics().contains(NodesInfoRequest.Metric.PLUGINS.metricName())) {
            metrics.add("plugins");
        }
        if (request.requestedMetrics().contains(NodesInfoRequest.Metric.PROCESS.metricName())) {
            metrics.add("process");
        }
        if (request.requestedMetrics().contains(NodesInfoRequest.Metric.SETTINGS.metricName())) {
            metrics.add("settings");
        }
        if (request.requestedMetrics().contains(NodesInfoRequest.Metric.THREAD_POOL.metricName())) {
            metrics.add("thread_pool");
        }
        if (request.requestedMetrics().contains(NodesInfoRequest.Metric.TRANSPORT.metricName())) {
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
