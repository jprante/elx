package org.xbib.elasticsearch.client.transport;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportStats;

import java.io.Closeable;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;

public interface Transport extends LifecycleComponent {

    Setting<Boolean> TRANSPORT_TCP_COMPRESS = Setting.boolSetting("transport.tcp.compress", false, Property.NodeScope);

    void setTransportService(TransportService service);

    /**
     * The address the transport is bound on.
     */
    BoundTransportAddress boundAddress();

    /**
     * Further profile bound addresses
     * @return <code>null</code> iff profiles are unsupported, otherwise a map with name of profile and its bound transport address
     */
    Map<String, BoundTransportAddress> profileBoundAddresses();

    /**
     * Returns an address from its string representation.
     */
    TransportAddress[] addressesFromString(String address, int perAddressLimit) throws UnknownHostException;

    /**
     * Returns <tt>true</tt> if the node is connected.
     */
    boolean nodeConnected(DiscoveryNode node);

    /**
     * Connects to a node with the given connection profile. If the node is already connected this method has no effect.
     * Once a successful is established, it can be validated before being exposed.
     */
    void connectToNode(DiscoveryNode node, ConnectionProfile connectionProfile,
                       CheckedBiConsumer<Connection, ConnectionProfile, IOException> connectionValidator) throws ConnectTransportException;

    /**
     * Disconnected from the given node, if not connected, will do nothing.
     */
    void disconnectFromNode(DiscoveryNode node);

    List<String> getLocalAddresses();

    default CircuitBreaker getInFlightRequestBreaker() {
        return new NoopCircuitBreaker("in-flight-noop");
    }

    /**
     * Returns a new request ID to use when sending a message via {@link Connection#sendRequest(long, String,
     * TransportRequest, TransportRequestOptions)}
     */
    long newRequestId();

    Connection getConnection(DiscoveryNode node);

    /**
     * Opens a new connection to the given node and returns it. In contrast to
     * {@link #connectToNode(DiscoveryNode, ConnectionProfile, CheckedBiConsumer)} the returned connection is not managed by
     * the transport implementation. This connection must be closed once it's not needed anymore.
     * This connection type can be used to execute a handshake between two nodes before the node will be published via
     * {@link #connectToNode(DiscoveryNode, ConnectionProfile, CheckedBiConsumer)}.
     */
    Connection openConnection(DiscoveryNode node, ConnectionProfile profile) throws IOException;

    TransportStats getStats();

    /**
     * A unidirectional connection to a {@link DiscoveryNode}
     */
    interface Connection extends Closeable {
        /**
         * The node this connection is associated with
         */
        DiscoveryNode getNode();

        void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options) throws
            IOException, TransportException;

        /**
         * Returns the version of the node this connection was established with.
         */
        default Version getVersion() {
            return getNode().getVersion();
        }

        /**
         * Returns a key that this connection can be cached on. Delegating subclasses must delegate method call to
         * the original connection.
         */
        default Object getCacheKey() {
            return this;
        }
    }
}
