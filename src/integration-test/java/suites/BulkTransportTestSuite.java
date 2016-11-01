package suites;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.xbib.elasticsearch.extras.client.transport.BulkTransportClientTest;
import org.xbib.elasticsearch.extras.client.transport.BulkTransportDuplicateIDTest;
import org.xbib.elasticsearch.extras.client.transport.BulkTransportReplicaTest;
import org.xbib.elasticsearch.extras.client.transport.BulkTransportUpdateReplicaLevelTest;

/**
 *
 */
@RunWith(ListenerSuite.class)
@Suite.SuiteClasses({
        BulkTransportClientTest.class,
        BulkTransportDuplicateIDTest.class,
        BulkTransportReplicaTest.class,
        BulkTransportUpdateReplicaLevelTest.class
})
public class BulkTransportTestSuite {

}
