package orj.johnsoft.reactive;

import reactor.core.publisher.Mono;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.http.Protocol;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest;
import software.amazon.awssdk.services.kinesis.model.KinesisException;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;

import java.net.URI;
import java.util.Properties;

public class KinesisService {

    public static final String MESSAGES = "messages";
    public static final String CONFIG_FILE = "config.properties";
    public static final String DEFAULT_KINESIS_ENDPOINT = "https://kinesis.us-east-1.amazonaws.com";
    private final KinesisAsyncClient kinesisClient;
    private String streamName;

    public KinesisService() {
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
        kinesisClient.createStream(CreateStreamRequest.builder()
                .streamName(MESSAGES)
                .shardCount(1)
                .build())
                .thenApply(response -> {
                    System.out.println(streamName + " stream was created");
                    return response;
                })
                .exceptionally(throwable -> {
                    if (throwable instanceof KinesisException) {
                        System.out.println(throwable.getMessage());
                    } else {
                        //throwable.printStackTrace();
                    }
                    return null;
                }).join();
    }


    public Mono<String> send(String key, String message) {
        return Mono.fromFuture(
                kinesisClient.putRecord(createPutRecordRequest(key, message))
                            .thenApply(r -> r.sequenceNumber())
                            .exceptionally(e -> {
                                e.printStackTrace();
                                return null;
                            })
        );
    }

    private PutRecordRequest createPutRecordRequest(String key, String message) {
        return PutRecordRequest.builder()
                .streamName(streamName)
                .partitionKey(key)
                .data(SdkBytes.fromUtf8String(message))
                .build();
    }

}
