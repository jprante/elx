package org.xbib.elx.http.action.admin.cluster.node.info;

import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoAction;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.node.info.PluginsAndModules;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.http.HttpInfo;
import org.elasticsearch.ingest.IngestInfo;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.monitor.os.OsInfo;
import org.elasticsearch.monitor.process.ProcessInfo;
import org.elasticsearch.threadpool.ThreadPoolInfo;
import org.elasticsearch.transport.TransportInfo;
import org.xbib.elx.http.HttpAction;
import org.xbib.elx.http.HttpActionContext;
import org.xbib.netty.http.client.api.Request;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
    protected CheckedFunction<XContentParser, NodesInfoResponse, IOException> entityParser() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected NodesInfoResponse emptyResponse() {
        return new NodesInfoResponse();
    }

    /**
     * Broken.
     */
    @SuppressWarnings("unchecked")
    protected NodesInfoResponse createResponse(HttpActionContext<NodesInfoRequest, NodesInfoResponse> httpContext) {
        Map<String, Object> map = null;
        //String string = (String)map.get("cluster_name");
        ClusterName clusterName = new ClusterName("");
        List<NodeInfo> nodeInfoList = new LinkedList<>();
        //map = (Map<String, Object>)map.get("nodes");

        for (Map.Entry<String, Object> entry : map.entrySet()) {
            String nodeId = entry.getKey();
            String ephemeralId = null;
            Map<String,Object> map2 = (Map<String, Object>) entry.getValue();
            String nodeName = (String) map2.get("name");
            String hostName = (String) map2.get("host");
            String hostAddress = (String) map2.get("ip");
            // <host>[/<ip>][:<port>]
            String transportAddressString = (String) map2.get("transport_address");
            int pos = transportAddressString.indexOf(':');
            String host = pos > 0 ? transportAddressString.substring(0, pos) : transportAddressString;
            int port = Integer.parseInt(pos > 0 ?  transportAddressString.substring(pos + 1) : "0");
            pos = host.indexOf('/');
            host = pos > 0 ? host.substring(0, pos) : host;
            try {
                InetAddress[] inetAddresses = InetAddress.getAllByName(host);
                TransportAddress transportAddress = new TransportAddress(inetAddresses[0], port);
                Build build = new Build(Build.Flavor.OSS, Build.Type.TAR,
                        (String) map2.get("build"),
                        (String) map2.get("date"),
                        (Boolean) map2.get("snapshot"));
                Map<String, String> attributes = Collections.emptyMap();
                Set<DiscoveryNode.Role> roles = new HashSet<>();
                Version version = Version.fromString((String) map2.get("version"));
                DiscoveryNode discoveryNode = new DiscoveryNode(nodeName, nodeId, ephemeralId, hostName, hostAddress,
                        transportAddress,
                        attributes, roles, version);
                /*Map<String, String> settingsMap = map2.containsKey("settings") ?
                        XContentHelper.
                        SettingsLoader.Helper.loadNestedFromMap((Map<String, Object>) map2.get("settings")) :
                        Collections.emptyMap();

                Settings settings = Settings.builder()

                        .put(settingsMap)
                        .build();*/
                OsInfo os = null;
                ProcessInfo processInfo = null;
                JvmInfo jvmInfo = null;
                ThreadPoolInfo threadPoolInfo = null;
                TransportInfo transportInfo = null;
                HttpInfo httpInfo = null;
                PluginsAndModules pluginsAndModules = null;
                IngestInfo ingestInfo = null;
                ByteSizeValue totalIndexingBuffer = null;
                NodeInfo nodeInfo = new NodeInfo(version,
                        build,
                        discoveryNode,
                        //serviceAttributes,
                        //settings,
                        null,
                        os, processInfo, jvmInfo, threadPoolInfo, transportInfo, httpInfo, pluginsAndModules,
                        ingestInfo,
                        totalIndexingBuffer);
                nodeInfoList.add(nodeInfo);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        List<FailedNodeException> failures = null;
        return new NodesInfoResponse(clusterName, nodeInfoList, failures);
    }
}
