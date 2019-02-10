package org.xbib.elasticsearch.client.transport;

import org.elasticsearch.cluster.node.DiscoveryNode;

public interface TransportConnectionListener {

    /**
     * Called once a node connection is opened and registered.
     */
    default void onNodeConnected(DiscoveryNode node) {}

    /**
     * Called once a node connection is closed and unregistered.
     */
    default void onNodeDisconnected(DiscoveryNode node) {}

    /**
     * Called once a node connection is closed. The connection might not have been registered in the
     * transport as a shared connection to a specific node
     */
    default void onConnectionClosed(Transport.Connection connection) {}

    /**
     * Called once a node connection is opened.
     */
    default void onConnectionOpened(Transport.Connection connection) {}
}
