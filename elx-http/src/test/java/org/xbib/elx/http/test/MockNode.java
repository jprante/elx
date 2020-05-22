package org.xbib.elx.http.test;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;

public class MockNode extends Node {

    public MockNode(Settings settings) {
        super(settings);
    }

}
