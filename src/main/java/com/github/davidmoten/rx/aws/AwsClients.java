package com.github.davidmoten.rx.aws;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;

public final class AwsClients {

    private AwsClients() {
        // prevent instantiation
    }

    public static AmazonSQS createSqsClient(AWSCredentials credentials, ClientConfiguration cc, Regions region) {
        return AmazonSQSClientBuilder //
                .standard() //
                .withCredentials(new AWSStaticCredentialsProvider(credentials)) //
                .withRegion(region) //
                .build();
    }

    public static AmazonS3 createS3Client(AWSCredentials credentials, ClientConfiguration cc, Regions region) {
        return AmazonS3ClientBuilder //
                .standard() //
                .withCredentials(new AWSStaticCredentialsProvider(credentials)) //
                .withRegion(region) //
                .build();
    }
}
