package org.xbib.elasticsearch.client.transport;

import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;

/**
 * This interface allows plugins to intercept requests on both the sender and the receiver side.
 */
public interface TransportInterceptor {

    default <T extends TransportRequest> TransportRequestHandler<T> interceptHandler(String action, String executor,
                                                                                     boolean forceExecution,
                                                                                     TransportRequestHandler<T> actualHandler) {
        return actualHandler;
    }


    default AsyncSender interceptSender(AsyncSender sender) {
        return sender;
    }


    interface AsyncSender {
        <T extends TransportResponse> void sendRequest(Transport.Connection connection, String action,
                                                       TransportRequest request, TransportRequestOptions options,
                                                       TransportResponseHandler<T> handler);
    }
}
