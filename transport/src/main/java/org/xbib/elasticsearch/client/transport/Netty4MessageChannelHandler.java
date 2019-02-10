package org.xbib.elasticsearch.client.transport;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.Attribute;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.transport.TcpHeader;
import org.elasticsearch.transport.Transports;

import java.net.InetSocketAddress;

/**
 * A handler (must be the last one!) that does size based frame decoding and forwards the actual message
 * to the relevant action.
 */
final class Netty4MessageChannelHandler extends ChannelDuplexHandler {

    private final Netty4Transport transport;
    private final String profileName;

    Netty4MessageChannelHandler(Netty4Transport transport, String profileName) {
        this.transport = transport;
        this.profileName = profileName;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        Transports.assertTransportThread();
        if (!(msg instanceof ByteBuf)) {
            ctx.fireChannelRead(msg);
            return;
        }
        final ByteBuf buffer = (ByteBuf) msg;
        final int remainingMessageSize = buffer.getInt(buffer.readerIndex() - TcpHeader.MESSAGE_LENGTH_SIZE);
        final int expectedReaderIndex = buffer.readerIndex() + remainingMessageSize;
        try {
            Channel channel = ctx.channel();
            InetSocketAddress remoteAddress = (InetSocketAddress) channel.remoteAddress();
            // netty always copies a buffer, either in NioWorker in its read handler, where it copies to a fresh
            // buffer, or in the cumulative buffer, which is cleaned each time so it could be bigger than the actual size
            BytesReference reference = Netty4Utils.toBytesReference(buffer, remainingMessageSize);
            Attribute<NettyTcpChannel> channelAttribute = channel.attr(Netty4Transport.CHANNEL_KEY);
            transport.messageReceived(reference, channelAttribute.get(), profileName, remoteAddress, remainingMessageSize);
        } finally {
            // Set the expected position of the buffer, no matter what happened
            buffer.readerIndex(expectedReaderIndex);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        Netty4Utils.maybeDie(cause);
        transport.exceptionCaught(ctx, cause);
    }

}
