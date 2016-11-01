package suites;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.xbib.elasticsearch.AliasTest;
import org.xbib.elasticsearch.SearchTest;
import org.xbib.elasticsearch.SimpleTest;
import org.xbib.elasticsearch.WildcardTest;

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
