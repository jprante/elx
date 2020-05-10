package org.xbib.elx.node.test;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.InternalSettingsPreparer;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;

import java.util.List;
import java.util.Map;

public class MockNode extends Node {

    public MockNode(Settings settings, List<Class<? extends Plugin>> classpathPlugins) {
        super(InternalSettingsPreparer.prepareEnvironment(settings, Map.of(), null, () -> "mock"),
                classpathPlugins, false);
    }
}
