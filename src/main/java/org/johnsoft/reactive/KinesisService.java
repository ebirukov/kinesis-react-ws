package org.johnsoft.reactive;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.http.Protocol;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.*;

import java.net.URI;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.*;

public class KinesisService {

    public static final String MESSAGES = "messages";
    public static final String CONFIG_FILE = "config.properties";
    public static final String DEFAULT_KINESIS_ENDPOINT = "https://kinesis.us-east-1.amazonaws.com";
    private final KinesisAsyncClient kinesisClient;
    private String streamName;

    private static final Logger log = LoggerFactory.getLogger(KinesisService.class);

    public KinesisService() {
        System.setProperty("aws.cborEnabled", "false");
        //System.setProperty("aws.cborEnabled", "false");
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
/*                .exceptionally(throwable -> {
                    if (throwable instanceof KinesisException) {
                        log.error("can't create stream " + streamName, throwable);
                    } else {
                        log.error("unkhown error while creating stream " + streamName, throwable);
                    }
                    return null;
                })*/
        ).onErrorResume(ResourceInUseException.class, e -> Mono.empty())
        .block(Duration.ofMillis(1000));
    }

    public Mono<ByteBuffer> send(List<ByteBuffer> messages) {
        return Mono.fromFuture(
                kinesisClient.putRecords(createPutRecordsRequest(messages))
                        .thenApply(this::processResult)
        ).onErrorStop()
                .onErrorReturn(KinesisException.class, ByteBuffer.wrap(("FAILURE").getBytes()));
    }

    private ByteBuffer processResult(PutRecordsResponse response) {
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
        System.out.println(numOfErrorByType);
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

    private PutRecordsRequest createPutRecordsRequest(List<ByteBuffer> messages) {
        return PutRecordsRequest.builder()
                .streamName(streamName)
                .records(createPutRecordsRequestsBuilder(messages))
                .build();
    }

    private Collection<PutRecordsRequestEntry> createPutRecordsRequestsBuilder(List<ByteBuffer> messages) {
        return messages.stream()
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
