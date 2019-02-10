package org.xbib.elasticsearch.client.transport;

import org.elasticsearch.Version;
import org.elasticsearch.transport.TcpChannel;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseOptions;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public final class TcpTransportChannel implements TransportChannel {
    private final TcpTransport transport;
    private final Version version;
    private final String action;
    private final long requestId;
    private final String profileName;
    private final long reservedBytes;
    private final AtomicBoolean released = new AtomicBoolean();
    private final String channelType;
    private final TcpChannel channel;

    TcpTransportChannel(TcpTransport transport, TcpChannel channel, String channelType, String action,
                        long requestId, Version version, String profileName, long reservedBytes) {
        this.version = version;
        this.channel = channel;
        this.transport = transport;
        this.action = action;
        this.requestId = requestId;
        this.profileName = profileName;
        this.reservedBytes = reservedBytes;
        this.channelType = channelType;
    }

    @Override
    public String getProfileName() {
        return profileName;
    }

    @Override
    public void sendResponse(TransportResponse response) throws IOException {
        sendResponse(response, TransportResponseOptions.EMPTY);
    }

    @Override
    public void sendResponse(TransportResponse response, TransportResponseOptions options) throws IOException {
        try {
            transport.sendResponse(version, channel, response, requestId, action, options);
        } finally {
            release(false);
        }
    }

    @Override
    public void sendResponse(Exception exception) throws IOException {
        try {
            transport.sendErrorResponse(version, channel, exception, requestId, action);
        } finally {
            release(true);
        }
    }

    private Exception releaseBy;

    private void release(boolean isExceptionResponse) {
        if (released.compareAndSet(false, true)) {
            assert (releaseBy = new Exception()) != null; // easier to debug if it's already closed
            transport.getInFlightRequestBreaker().addWithoutBreaking(-reservedBytes);
        } else if (isExceptionResponse == false) {
            // only fail if we are not sending an error - we might send the error triggered by the previous
            // sendResponse call
            throw new IllegalStateException("reserved bytes are already released", releaseBy);
        }
    }

    @Override
    public String getChannelType() {
        return channelType;
    }

    @Override
    public Version getVersion() {
        return version;
    }

    public TcpChannel getChannel() {
        return channel;
    }
}

