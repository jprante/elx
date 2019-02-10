package org.xbib.elasticsearch.client.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Collections;
import java.util.Enumeration;

public class NetworkTest {

    private static final Logger logger = LogManager.getLogger(NetworkTest.class);

    /**
     * Demonstrates the slowness oj Java network interface lookup on certain environments.
     * May be a killer for ES node startup - so avoid automatic traversal of NICs at all costs.
     *
     * @throws Exception if test fails
     */
    @Test
    public void testNetwork() throws Exception {
        Enumeration<NetworkInterface> nets = NetworkInterface.getNetworkInterfaces();
        for (NetworkInterface netint : Collections.list(nets)) {
            logger.info("checking network interface = " + netint.getName());
            Enumeration<InetAddress> inetAddresses = netint.getInetAddresses();
            for (InetAddress addr : Collections.list(inetAddresses)) {
                logger.info("found address = " + addr.getHostAddress()
                        + " name = " + addr.getHostName()
                        + " canicalhostname = " + addr.getCanonicalHostName()
                        + " loopback = " + addr.isLoopbackAddress()
                        + " sitelocal = " + addr.isSiteLocalAddress()
                        + " linklocal = " + addr.isLinkLocalAddress()
                        + " anylocal = " + addr.isAnyLocalAddress()
                        + " multicast = " + addr.isMulticastAddress()
                        + " mcglobal = " + addr.isMCGlobal()
                        + " mclinklocal = " + addr.isMCLinkLocal()
                        + " mcnodelocal = " + addr.isMCNodeLocal()
                        + " mcorglocal = " + addr.isMCOrgLocal()
                        + " mcsitelocal = " + addr.isMCSiteLocal()
                        + " mcsitelocal = " + addr.isReachable(1000));
            }
        }

    }
}
