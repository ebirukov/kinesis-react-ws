package org.johnsoft.reactive;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.netty.http.websocket.WebsocketInbound;
import reactor.netty.http.websocket.WebsocketOutbound;

import java.nio.ByteBuffer;
import java.util.UUID;

public class WebSocketCommunicationHandler {

    KinesisService kinesisService = new KinesisService();

    Publisher<Void> process(WebsocketInbound in, WebsocketOutbound out) {
        return out.send(in.receive()
                        .retain()
                        .doOnNext(this::send)
                        .map(Unpooled::wrappedBuffer)
                        //.log()
                        );
    }

    private Mono<ByteBuffer> send(ByteBuf byteBuf) {
        return kinesisService.send(UUID.randomUUID().toString(), byteBuf.nioBuffer());
    }
}
