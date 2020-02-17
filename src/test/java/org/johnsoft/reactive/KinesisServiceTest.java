package org.johnsoft.reactive;


import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;
import org.mockito.Mockito;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.util.UUID;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class KinesisServiceTest {

    @Test
    public void testSend() {

        KinesisService kinesisService = mock(KinesisService.class);
        ByteBuffer message = ByteBuffer.wrap("hello".getBytes());
        ByteBuffer result = ByteBuffer.wrap("response".getBytes());
        when(kinesisService.send(anyString(), any(ByteBuffer.class))).thenReturn(Mono.just(result));
        Mono<ByteBuffer> mono = kinesisService.send(UUID.randomUUID().toString(), message);
        assertNotNull(mono);
        assertNotNull(mono.block());
    }

}