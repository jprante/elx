package org.xbib.elx.common.test;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.InternalSettingsPreparer;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;

import java.util.List;

class MockNode extends Node {

    MockNode(Settings settings, List<Class<? extends Plugin>> classpathPlugins) {
        super(InternalSettingsPreparer.prepareEnvironment(settings, null), classpathPlugins);
    }
}
