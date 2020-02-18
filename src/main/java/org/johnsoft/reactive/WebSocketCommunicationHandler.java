package org.johnsoft.reactive;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.netty.http.websocket.WebsocketInbound;
import reactor.netty.http.websocket.WebsocketOutbound;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.*;

public class WebSocketCommunicationHandler {

    private final KinesisService kinesisService = new KinesisService();

    Publisher<Void> process(WebsocketInbound in, WebsocketOutbound out) {
        return out.send(in.receive()
                        .retain()
                        .buffer(500)
                        //.buffer(Duration.ofMillis(1))
                        .map(this::convert)
                        .map(kinesisService::send)
                        .flatMap(r -> r.map(Unpooled::wrappedBuffer))
                        .log()
                         );
    }

    private List<ByteBuffer> convert(List<ByteBuf> byteBuf) {
        return byteBuf
                .stream()
                .map(ByteBuf::nioBuffer)
                .collect(toList());
    }
}
