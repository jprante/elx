package org.xbib.elasticsearch.client;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.List;
import java.util.Locale;

public class NetworkUtils {

    private static final Logger logger = LogManager.getLogger(NetworkUtils.class.getName());

    private static final String IPV4_SETTING = "java.net.preferIPv4Stack";

    private static final String IPV6_SETTING = "java.net.preferIPv6Addresses";

    private static final InetAddress LOCAL_ADDRESS;

    static {
        InetAddress address;
        try {
            address = InetAddress.getLocalHost();
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
            address = InetAddress.getLoopbackAddress();
        }
        LOCAL_ADDRESS = address;
    }

    private NetworkUtils() {
    }

    public static InetAddress getLocalAddress() {
        return LOCAL_ADDRESS;
    }

    public static InetAddress getFirstNonLoopbackAddress(ProtocolVersion ipversion) throws SocketException {
        InetAddress address;
        for (NetworkInterface networkInterface : getNetworkInterfaces()) {
            try {
                if (!networkInterface.isUp() || networkInterface.isLoopback()) {
                    continue;
                }
            } catch (Exception e) {
                logger.warn(e.getMessage(), e);
                continue;
            }
            address = getFirstNonLoopbackAddress(networkInterface, ipversion);
            if (address != null) {
                return address;
            }
        }
        return null;
    }

    public static InetAddress getFirstNonLoopbackAddress(NetworkInterface networkInterface, ProtocolVersion ipVersion)
            throws SocketException {
        if (networkInterface == null) {
            throw new IllegalArgumentException("network interface is null");
        }
        for (Enumeration<InetAddress> addresses = networkInterface.getInetAddresses(); addresses.hasMoreElements(); ) {
            InetAddress address = addresses.nextElement();
            if (!address.isLoopbackAddress() && (address instanceof Inet4Address && ipVersion == ProtocolVersion.IPV4) ||
                        (address instanceof Inet6Address && ipVersion == ProtocolVersion.IPV6)) {
                return address;
            }
        }
        return null;
    }

    public static InetAddress getFirstAddress(NetworkInterface networkInterface, ProtocolVersion ipVersion)
            throws SocketException {
        if (networkInterface == null) {
            throw new IllegalArgumentException("network interface is null");
        }
        for (Enumeration<InetAddress> addresses = networkInterface.getInetAddresses(); addresses.hasMoreElements(); ) {
            InetAddress address = addresses.nextElement();
            if ((address instanceof Inet4Address && ipVersion == ProtocolVersion.IPV4) ||
                    (address instanceof Inet6Address && ipVersion == ProtocolVersion.IPV6)) {
                return address;
            }
        }
        return null;
    }

    public static List<NetworkInterface> getAllAvailableInterfaces() throws SocketException {
        List<NetworkInterface> allInterfaces = new ArrayList<>();
        for (Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
             interfaces.hasMoreElements(); ) {
            NetworkInterface networkInterface = interfaces.nextElement();
            allInterfaces.add(networkInterface);
            Enumeration<NetworkInterface> subInterfaces = networkInterface.getSubInterfaces();
            if (subInterfaces.hasMoreElements()) {
                while (subInterfaces.hasMoreElements()) {
                    allInterfaces.add(subInterfaces.nextElement());
                }
            }
        }
        sortInterfaces(allInterfaces);
        return allInterfaces;
    }

    public static List<InetAddress> getAllAvailableAddresses() throws SocketException {
        List<InetAddress> allAddresses = new ArrayList<>();
        for (NetworkInterface networkInterface : getNetworkInterfaces()) {
            Enumeration<InetAddress> addrs = networkInterface.getInetAddresses();
            while (addrs.hasMoreElements()) {
                allAddresses.add(addrs.nextElement());
            }
        }
        sortAddresses(allAddresses);
        return allAddresses;
    }

    public static ProtocolVersion getProtocolVersion() throws SocketException {
        switch (findAvailableProtocols()) {
            case IPV4:
                return ProtocolVersion.IPV4;
            case IPV6:
                return ProtocolVersion.IPV6;
            case IPV46:
                if (Boolean.getBoolean(System.getProperty(IPV4_SETTING))) {
                    return ProtocolVersion.IPV4;
                }
                if (Boolean.getBoolean(System.getProperty(IPV6_SETTING))) {
                    return ProtocolVersion.IPV6;
                }
                return ProtocolVersion.IPV6;
            default:
                break;
        }
        return ProtocolVersion.NONE;
    }

    public static ProtocolVersion findAvailableProtocols() throws SocketException {
        boolean hasIPv4 = false;
        boolean hasIPv6 = false;
        for (InetAddress addr : getAllAvailableAddresses()) {
            if (addr instanceof Inet4Address) {
                hasIPv4 = true;
            }
            if (addr instanceof Inet6Address) {
                hasIPv6 = true;
            }
        }
        if (hasIPv4 && hasIPv6) {
            return ProtocolVersion.IPV46;
        }
        if (hasIPv4) {
            return ProtocolVersion.IPV4;
        }
        if (hasIPv6) {
            return ProtocolVersion.IPV6;
        }
        return ProtocolVersion.NONE;
    }

    public static InetAddress resolveInetAddress(String hostname, String defaultValue) throws IOException {
        String host = hostname;
        if (host == null) {
            host = defaultValue;
        }
        String origHost = host;
        int pos = host.indexOf(':');
        if (pos > 0) {
            host = host.substring(0, pos - 1);
        }
        if ((host.startsWith("#") && host.endsWith("#")) || (host.startsWith("_") && host.endsWith("_"))) {
            host = host.substring(1, host.length() - 1);
            if ("local".equals(host)) {
                return getLocalAddress();
            } else if (host.startsWith("non_loopback")) {
                if (host.toLowerCase(Locale.ROOT).endsWith(":ipv4")) {
                    return getFirstNonLoopbackAddress(ProtocolVersion.IPV4);
                } else if (host.toLowerCase(Locale.ROOT).endsWith(":ipv6")) {
                    return getFirstNonLoopbackAddress(ProtocolVersion.IPV6);
                } else {
                    return getFirstNonLoopbackAddress(getProtocolVersion());
                }
            } else {
                ProtocolVersion protocolVersion = getProtocolVersion();
                if (host.toLowerCase(Locale.ROOT).endsWith(":ipv4")) {
                    protocolVersion = ProtocolVersion.IPV4;
                    host = host.substring(0, host.length() - 5);
                } else if (host.toLowerCase(Locale.ROOT).endsWith(":ipv6")) {
                    protocolVersion = ProtocolVersion.IPV6;
                    host = host.substring(0, host.length() - 5);
                }
                for (NetworkInterface ni : getAllAvailableInterfaces()) {
                    if (!ni.isUp()) {
                        continue;
                    }
                    if (host.equals(ni.getName()) || host.equals(ni.getDisplayName())) {
                        if (ni.isLoopback()) {
                            return getFirstAddress(ni, protocolVersion);
                        } else {
                            return getFirstNonLoopbackAddress(ni, protocolVersion);
                        }
                    }
                }
            }
            throw new IOException("failed to find network interface for [" + origHost + "]");
        }
        return InetAddress.getByName(host);
    }

    private static List<NetworkInterface> getNetworkInterfaces() throws SocketException {
        List<NetworkInterface> networkInterfaces = new ArrayList<>();
        Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
        while (interfaces.hasMoreElements()) {
            NetworkInterface networkInterface = interfaces.nextElement();
            networkInterfaces.add(networkInterface);
            Enumeration<NetworkInterface> subInterfaces = networkInterface.getSubInterfaces();
            if (subInterfaces.hasMoreElements()) {
                while (subInterfaces.hasMoreElements()) {
                    networkInterfaces.add(subInterfaces.nextElement());
                }
            }
        }
        sortInterfaces(networkInterfaces);
        return networkInterfaces;
    }

    private static void sortInterfaces(List<NetworkInterface> interfaces) {
        Collections.sort(interfaces, Comparator.comparingInt(NetworkInterface::getIndex));
    }

    private static void sortAddresses(List<InetAddress> addressList) {
        Collections.sort(addressList, (o1, o2) -> compareBytes(o1.getAddress(), o2.getAddress()));
    }

    private static int compareBytes(byte[] left, byte[] right) {
        for (int i = 0, j = 0; i < left.length && j < right.length; i++, j++) {
            int a = left[i] & 0xff;
            int b = right[j] & 0xff;
            if (a != b) {
                return a - b;
            }
        }
        return left.length - right.length;
    }

    public enum ProtocolVersion {
        IPV4, IPV6, IPV46, NONE
    }
}
