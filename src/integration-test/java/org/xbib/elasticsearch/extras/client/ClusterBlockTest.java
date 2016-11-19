package org.xbib.elasticsearch.extras.client;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.junit.Before;
import org.junit.Test;
import org.xbib.elasticsearch.NodeTestBase;

/**
 *
 */
public class ClusterBlockTest extends NodeTestBase {

    private static final Logger logger = LogManager.getLogger(ClusterBlockTest.class.getName());

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

    protected Settings getNodeSettings() {
        return Settings.builder()
                .put(super.getNodeSettings())
                .put("discovery.zen.minimum_master_nodes", 2) // block until we have two nodes
                .build();
    }

    @Test(expected = ClusterBlockException.class)
    public void testClusterBlock() throws Exception {
        BulkRequestBuilder brb = client("1").prepareBulk();
        XContentBuilder builder = jsonBuilder().startObject().field("field1", "value1").endObject();
        String jsonString = builder.string();
        IndexRequestBuilder irb = client("1").prepareIndex("test", "test", "1").setSource(jsonString);
        brb.add(irb);
        brb.execute().actionGet();
    }

}
