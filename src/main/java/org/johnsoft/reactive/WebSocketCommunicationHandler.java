package org.johnsoft.reactive;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.netty.http.websocket.WebsocketInbound;
import reactor.netty.http.websocket.WebsocketOutbound;
import reactor.util.function.Tuple2;

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
                                .flatMap(r -> r.map(Unpooled::wrappedBuffer))
                                .as(out::send)
                )
                .flatMap(Flux::from);
    }

    private List<ByteBuffer> convert(List<ByteBuf> byteBuf) {
        return byteBuf
                .stream()
                .map(this::toNioBuffer)
                .collect(toList());
    }

    private ByteBuffer toNioBuffer(ByteBuf bb) {
        ByteBuffer byteBuffer = bb.nioBuffer();
        bb.release();
        return byteBuffer;
    }
}
