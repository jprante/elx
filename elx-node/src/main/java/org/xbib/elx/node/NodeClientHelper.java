package org.xbib.elx.node;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeValidationException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.netty4.Netty4Plugin;
import org.xbib.elx.common.Parameters;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class NodeClientHelper {

    private static final Logger logger = LogManager.getLogger(NodeClientHelper.class.getName());

    private static Object configurationObject;

    private static Node node;

    private static final Map<String, ElasticsearchClient> clientMap = new HashMap<>();

    public ElasticsearchClient createClient(Settings settings, Object object) {
        if (configurationObject == null) {
            configurationObject = object;
        }
        if (configurationObject instanceof ElasticsearchClient) {
            return (ElasticsearchClient) configurationObject;
        }
        return clientMap.computeIfAbsent(settings.get("cluster.name"),
                key -> innerCreateClient(settings));
    }

    public void closeClient(Settings settings) throws IOException {
        ElasticsearchClient client = clientMap.remove(settings.get("cluster.name"));
        if (client != null) {
            logger.debug("closing node...");
            node.close();
            node = null;
        }
    }

    private ElasticsearchClient innerCreateClient(Settings settings) {
        String version = System.getProperty("os.name")
                + " " + System.getProperty("java.vm.name")
                + " " + System.getProperty("java.vm.vendor")
                + " " + System.getProperty("java.runtime.version")
                + " " + System.getProperty("java.vm.version");
        Settings effectiveSettings = Settings.builder()
                .put(settings.filter(key -> !isPrivateSettings(key)))
                .put("node.master", false)
                .put("node.data", false)
                // "node.processors"
                .put(EsExecutors.PROCESSORS_SETTING.getKey(),
                        settings.get(EsExecutors.PROCESSORS_SETTING.getKey(),
                                String.valueOf(Runtime.getRuntime().availableProcessors())))
                // "transport.type"
                .put(NetworkModule.TRANSPORT_TYPE_KEY,
                        Netty4Plugin.NETTY_TRANSPORT_NAME)
                .build();
        logger.info("creating node client on {} with effective settings {}",
                version, effectiveSettings.toDelimitedString(','));
        Collection<Class<? extends Plugin>> plugins =
                Collections.singletonList(Netty4Plugin.class);
        node = new BulkNode(new Environment(effectiveSettings, null), plugins);
        try {
            node.start();
            return node.client();
        } catch (NodeValidationException e) {
            logger.log(Level.ERROR, e.getMessage(), e);
        }
        return null;
    }

    private static boolean isPrivateSettings(String key) {
        return key.equals(Parameters.MAX_ACTIONS_PER_REQUEST.name()) ||
                key.equals(Parameters.MAX_CONCURRENT_REQUESTS.name()) ||
                key.equals(Parameters.MAX_VOLUME_PER_REQUEST.name()) ||
                key.equals(Parameters.FLUSH_INTERVAL.name());
    }

    private static class BulkNode extends Node {

        BulkNode(Environment env, Collection<Class<? extends Plugin>> classpathPlugins) {
            super(env, classpathPlugins);
        }
    }
}
