package suites;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.xbib.elasticsearch.extras.client.AliasTest;
import org.xbib.elasticsearch.extras.client.SearchTest;
import org.xbib.elasticsearch.extras.client.SimpleTest;
import org.xbib.elasticsearch.extras.client.WildcardTest;

/**
 *
 */
@RunWith(ListenerSuite.class)
@Suite.SuiteClasses({
        SimpleTest.class,
        AliasTest.class,
        SearchTest.class,
        WildcardTest.class
})
public class MiscTestSuite {
}
