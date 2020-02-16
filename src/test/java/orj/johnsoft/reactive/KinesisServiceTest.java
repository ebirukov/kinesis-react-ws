package orj.johnsoft.reactive;


import org.junit.Assert;
import org.junit.Test;
import reactor.core.publisher.Mono;

import java.util.UUID;

import static org.junit.Assert.*;

public class KinesisServiceTest {

    @Test
    public void testSend() {
        KinesisService kinesisService = new KinesisService();
        Mono<String> mono = kinesisService.send(UUID.randomUUID().toString(), "hello");
        assertNotNull(mono);
        assertNotNull(mono.block());
    }

}