package org.xbib.elx.node;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

@Ignore
public class ExtendedNodeClusterBlockTest extends NodeTestUtils {

    private static final Logger logger = LogManager.getLogger("test");

    @Before
    public void startNodes() {
        try {
            setClusterName();
            startNode("1");
            findNodeAddress();
            // do not wait for green health state
            logger.info("ready");
        } catch (Throwable t) {
            logger.error("startNodes failed", t);
        }
    }

    @Override
    protected Settings getNodeSettings() {
        return Settings.settingsBuilder()
                .put(super.getNodeSettings())
                .put("discovery.zen.minimum_master_nodes", 2) // block until we have two nodes
                .build();
    }

    @Test(expected = ClusterBlockException.class)
    public void testClusterBlock() throws Exception {
        BulkRequestBuilder brb = client("1").prepareBulk();
        XContentBuilder builder = jsonBuilder().startObject().field("field1", "value1").endObject();
        IndexRequestBuilder irb = client("1").prepareIndex("test", "test", "1").setSource(builder);
        brb.add(irb);
        brb.execute().actionGet();
    }

}
