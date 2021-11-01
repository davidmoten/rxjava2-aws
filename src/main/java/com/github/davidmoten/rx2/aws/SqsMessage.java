package com.github.davidmoten.rx2.aws;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.Callable;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.sqs.AmazonSQS;
import com.github.davidmoten.guavamini.Preconditions;

public final class SqsMessage {

    private final String messageReceiptHandle;
    private final byte[] bytes;
    private final long timestamp;
    private final Optional<String> s3Id;
    private final Service service;

    SqsMessage(String messageReceiptHandle, byte[] bytes, long timestamp, Optional<String> s3Id, Service service) {
        this.messageReceiptHandle = messageReceiptHandle;
        this.bytes = bytes;
        this.timestamp = timestamp;
        this.s3Id = s3Id;
        this.service = service;
    }
    
    public String messageReceiptHandle() {
        return messageReceiptHandle;
    }
    
    public Optional<String> s3Id() {
        return s3Id;
    }

    public byte[] bytes() {
        return bytes;
    }

    public String message() {
        return new String(bytes(), StandardCharsets.UTF_8);
    }

    public long lastModifiedTime() {
        return timestamp;
    }

    public void deleteMessage() {
        try {
            deleteMessage(Client.FROM_SOURCE);
        } catch (final RuntimeException e) {
            deleteMessage(Client.FROM_FACTORY);
        }
    }

    public void deleteMessage(Client client) {
        if (client == Client.FROM_SOURCE) {
            deleteMessage(service.s3, service.sqs);
        } else {
            deleteMessageUsingFactory(service.s3Factory, service.sqsFactory);
        }
    }

    private void deleteMessageUsingFactory(Optional<Callable<AmazonS3>> s3Factory, Callable<AmazonSQS> sqsFactory) {
        final Optional<AmazonS3> s3 = s3Factory.map(Util::uncheckedCall);
        AmazonSQS sqs = Util.uncheckedCall(sqsFactory);
        try {
            deleteMessage(s3, sqs);
        } finally {
            s3.ifPresent(Util::shutdown);
            Util.shutdown(sqs);
        }
    }

    public void deleteMessage(Optional<AmazonS3> s3, AmazonSQS sqs) {
        Preconditions.checkArgument(!s3.isPresent() || s3Id.isPresent(), "s3Id must be present");
        if (s3Id.isPresent()) {
            s3.get().deleteObject(service.bucketName.get(), s3Id.get());
        }
        sqs.deleteMessage(service.queueUrl, messageReceiptHandle);
    }

    @Override
    public String toString() {
        return "MessageAndBytes [messageReceiptHandle=" + messageReceiptHandle + ", bytes=" + Arrays.toString(bytes)
                + ", timestamp=" + timestamp + ", s3Id=" + s3Id + ", bucketName=" + service.bucketName + ", queueName="
                + service.queueUrl + "]";
    }

    public static enum Client {
        FROM_SOURCE, FROM_FACTORY;
    }

    static class Service {

        final Callable<AmazonSQS> sqsFactory;
        final Optional<Callable<AmazonS3>> s3Factory;
        final Optional<AmazonS3> s3;
        final AmazonSQS sqs;
        final String queueUrl;
        final Optional<String> bucketName;

        Service(Optional<Callable<AmazonS3>> s3Factory, Callable<AmazonSQS> sqsFactory, Optional<AmazonS3> s3,
                AmazonSQS sqs, String queueUrl, Optional<String> bucketName) {
            Preconditions.checkNotNull(s3Factory);
            Preconditions.checkNotNull(sqsFactory);
            Preconditions.checkNotNull(s3);
            Preconditions.checkNotNull(sqs);
            Preconditions.checkNotNull(queueUrl);
            Preconditions.checkNotNull(bucketName);
            this.s3Factory = s3Factory;
            this.sqsFactory = sqsFactory;
            this.s3 = s3;
            this.sqs = sqs;
            this.queueUrl = queueUrl;
            this.bucketName = bucketName;
        }

    }

}