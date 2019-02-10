package org.xbib.elasticsearch.client.transport;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.TooLongFrameException;
import org.elasticsearch.transport.TcpHeader;

import java.util.List;

final class Netty4SizeHeaderFrameDecoder extends ByteToMessageDecoder {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        try {
            boolean continueProcessing = TcpTransport.validateMessageHeader(Netty4Utils.toBytesReference(in));
            final ByteBuf message = in.skipBytes(TcpHeader.MARKER_BYTES_SIZE + TcpHeader.MESSAGE_LENGTH_SIZE);
            if (!continueProcessing) return;
            out.add(message);
        } catch (IllegalArgumentException ex) {
            throw new TooLongFrameException(ex);
        } catch (IllegalStateException ex) {
            /* decode will be called until the ByteBuf is fully consumed; when it is fully
             * consumed, transport#validateMessageHeader will throw an IllegalStateException which
             * is okay, it means we have finished consuming the ByteBuf and we can get out
             */
        }
    }

}
