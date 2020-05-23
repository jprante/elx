package org.xbib.elx.node;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;
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

    public void closeClient(Settings settings) {
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
        Settings effectiveSettings = Settings.builder().put(settings)
                .put("node.client", true)
                .put("node.master", false)
                .put("node.data", false)
                .build();
        logger.info("creating node client on {} with effective settings {}",
                version, effectiveSettings.getAsMap());
        Collection<Class<? extends Plugin>> plugins = Collections.emptyList();
        node = new BulkNode(new Environment(effectiveSettings), plugins);
        node.start();
        return node.client();
    }

    private static class BulkNode extends Node {

        BulkNode(Environment env, Collection<Class<? extends Plugin>> classpathPlugins) {
            super(env, Version.CURRENT, classpathPlugins);
        }
    }
}
