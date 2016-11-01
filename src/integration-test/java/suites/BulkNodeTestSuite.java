package suites;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.xbib.elasticsearch.extras.client.node.BulkNodeClientTest;
import org.xbib.elasticsearch.extras.client.node.BulkNodeDuplicateIDTest;
import org.xbib.elasticsearch.extras.client.node.BulkNodeIndexAliasTest;
import org.xbib.elasticsearch.extras.client.node.BulkNodeReplicaTest;
import org.xbib.elasticsearch.extras.client.node.BulkNodeUpdateReplicaLevelTest;

/**
 *
 */
@RunWith(ListenerSuite.class)
@Suite.SuiteClasses({
        BulkNodeClientTest.class,
        BulkNodeDuplicateIDTest.class,
        BulkNodeReplicaTest.class,
        BulkNodeUpdateReplicaLevelTest.class,
        BulkNodeIndexAliasTest.class
})
public class BulkNodeTestSuite {
}
