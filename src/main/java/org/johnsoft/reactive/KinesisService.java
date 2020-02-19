package org.johnsoft.reactive;

import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.exception.SdkInterruptedException;
import software.amazon.awssdk.http.Protocol;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.*;

import java.net.URI;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.*;

public class KinesisService {

    public static final String MESSAGES = "messages";
    public static final String CONFIG_FILE = "config.properties";
    public static final String DEFAULT_KINESIS_ENDPOINT = "https://kinesis.us-east-1.amazonaws.com";
    private final KinesisAsyncClient kinesisClient;
    private String streamName;
    AtomicLong counter = new AtomicLong();

    private static final Logger log = LoggerFactory.getLogger(KinesisService.class);

    public KinesisService() {
        System.setProperty("aws.cborEnabled", "false");
        Properties config = ApplicationProperties.loadProperties(CONFIG_FILE);

        String endpoint = config.getProperty("kinesis.endpoint", DEFAULT_KINESIS_ENDPOINT);
        this.kinesisClient = KinesisAsyncClient.builder()
                .httpClientBuilder(NettyNioAsyncHttpClient.builder()
                        .protocol(Protocol.HTTP1_1))
                .endpointOverride(URI.create(endpoint))
                .build();
        streamName = config.getProperty("kinesis.streamName");
        createStreamIfNotExists();
    }

    private void createStreamIfNotExists() {
        Mono.fromFuture(
        kinesisClient.createStream(CreateStreamRequest.builder()
                .streamName(MESSAGES)
                .shardCount(1)
                .build())
                .thenApply(response -> {
                    log.info(streamName + " stream was created");
                    return response;
                })
        ).onErrorResume(ResourceInUseException.class, e -> Mono.empty())
        .block(Duration.ofMillis(1000));
    }

    public Mono<ByteBuffer> send(Flux<ByteBuffer> messages) {
        return send(messages.toStream());
    }

    public Mono<ByteBuffer> send(List<ByteBuffer> messages) {
        return send(messages.stream());
    }

    private Mono<ByteBuffer> send(Stream messageStream) {
        return Mono.fromFuture(
                kinesisClient.putRecords(createPutRecordsRequest(messageStream))
                        .thenApply(this::processResult)
        ).onErrorStop()
                .onErrorReturn(SdkClientException.class, ByteBuffer.wrap(("TRY AGAIN").getBytes()))
                //.onErrorReturn(SdkInterruptedException.class, ByteBuffer.wrap(("Interrupted").getBytes()))
                .onErrorReturn(KinesisException.class, ByteBuffer.wrap(("FAILURE").getBytes()));
    }

    private ByteBuffer processResult(PutRecordsResponse response) {
        if (response.failedRecordCount() == 0) return ByteBuffer.wrap(String.valueOf(response.records().size()).getBytes());
        return ByteBuffer.wrap(response.records()
                .stream()
                .filter(r -> r.errorCode() != null)
                .collect(collectingAndThen(
                        groupingBy(PutRecordsResultEntry::errorCode,
                                mapping(PutRecordsResultEntry::errorCode, counting())),
                        this::formatErrorResult
                )).getBytes()
        );

    }

    private String formatErrorResult(Map<String, Long> numOfErrorByType) {
        StringJoiner joiner = new StringJoiner(",");
        numOfErrorByType.forEach((type, num) -> joiner.add(String.join("-", type, num.toString())));
        return joiner.toString();
    }

    public Mono<ByteBuffer> send(ByteBuffer message) {
        return Mono.fromFuture(
                kinesisClient.putRecord(createPutRecordRequest(message))
                            .thenApply(this::convertResult)
        );
    }

    private ByteBuffer convertResult(PutRecordResponse r) {
        return ByteBuffer.wrap(r.sequenceNumber().getBytes());
    }

    private PutRecordsRequest createPutRecordsRequest(Stream<ByteBuffer> messageStream) {
        return PutRecordsRequest.builder()
                .streamName(streamName)
                .records(createPutRecordsRequestsBuilder(messageStream))
                .build();
    }

    private Collection<PutRecordsRequestEntry> createPutRecordsRequestsBuilder(Stream<ByteBuffer> messageStream) {
        return messageStream
                .map(m -> PutRecordsRequestEntry.builder()
                        .partitionKey(UUID.randomUUID().toString())
                        .data(SdkBytes.fromByteBuffer(m))
                        .build())
                .collect(Collectors.toList());
    }

    private PutRecordRequest createPutRecordRequest(ByteBuffer message) {
        return PutRecordRequest.builder()
                .streamName(streamName)
                .data(SdkBytes.fromByteBuffer(message))
                .build();
    }

}
