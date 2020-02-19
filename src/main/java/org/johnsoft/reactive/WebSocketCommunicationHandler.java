package org.johnsoft.reactive;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.netty.http.websocket.WebsocketInbound;
import reactor.netty.http.websocket.WebsocketOutbound;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;

import static java.util.stream.Collectors.toList;

public class WebSocketCommunicationHandler {

    private final KinesisService kinesisService = new KinesisService();

    Publisher<Void> process(WebsocketInbound in, WebsocketOutbound out) {
        return in.receive()
                .retain()
                .window(Duration.ofMillis(10))
                .map(w -> w.buffer(500)
                                .map(this::convert)
                                .map(kinesisService::send)
                                .onBackpressureBuffer(500)
                                //.log("", Level.INFO, false, SignalType.ON_ERROR)
                                .flatMap(r -> r.map(Unpooled::wrappedBuffer))
                                .publish(out::send)
                ).doOnCancel(() -> System.out.println("cancel"))
                .doOnError(e -> e.printStackTrace())
                .flatMap(Flux::from);
    }

    private List<ByteBuffer> convert(List<ByteBuf> byteBuf) {
        return byteBuf
                .stream()
                .map(ByteBuf::nioBuffer)
                .collect(toList());
    }
}
