package com.github.davidmoten.rx.aws;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.sqs.AmazonSQSClient;

public class AwsClients {

    public static AmazonSQSClient createSqsClient(AWSCredentials credentials,
            ClientConfiguration cc, Regions region) {
        Region reg = Region.getRegion(region);
        return new AmazonSQSClient(credentials, cc).withRegion(reg);
    }

    public static AmazonS3Client createS3Client(AWSCredentials credentials, ClientConfiguration cc,
            Regions region) {
        Region reg = Region.getRegion(region);
        return new AmazonS3Client(credentials, cc).withRegion(reg);
    }
}
