package com.automq.rocketmq.test.factory;

import java.net.URI;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

public class LocalStackFactory {
    private static final DockerImageName LOCALSTACK_IMAGE;
    private static final LocalStackContainer LOCALSTACK;

    static {
        LOCALSTACK_IMAGE = DockerImageName.parse("localstack/localstack:0.11.3");
        LOCALSTACK = new LocalStackContainer(LOCALSTACK_IMAGE).withServices(S3);
    }

    public static LocalStackContainer getInstance() {
        return LOCALSTACK;
    }

    /**
     * After LocalStack start.
     * 如果没有容器启动，则先启动容器。
     * <br>
     * 创建一个bucket，名为 "test-bucket"
     */
    public static void createBucket() {
        if (!LOCALSTACK.isRunning()) {
            LOCALSTACK.start();
        }
        String accessKey = LOCALSTACK.getAccessKey();
        String secretKey = LOCALSTACK.getSecretKey();
        Region region = Region.of(LOCALSTACK.getRegion());
        URI endpoint = LOCALSTACK.getEndpoint();
        AwsBasicCredentials basicCredentials = AwsBasicCredentials.create(accessKey, secretKey);
        AwsCredentialsProvider credentialsProvider = StaticCredentialsProvider.create(basicCredentials);
        S3Client s3 = S3Client.builder()
            .credentialsProvider(credentialsProvider)
            .region(region)
            .endpointOverride(endpoint)
            .build();

        CreateBucketRequest createBucketRequest = CreateBucketRequest.builder().bucket("test-bucket").build();
        s3.createBucket(createBucketRequest);
    }
}
