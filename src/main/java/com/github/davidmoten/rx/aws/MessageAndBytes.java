package com.github.davidmoten.rx.aws;
import java.util.Arrays;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;

public final class MessageAndBytes {

    private final String messageReceiptHandle;
    private final byte[] bytes;
    private final long timestamp;
    private final String s3Id;
    private final String bucketName;
    private final String queueName;
    private final AmazonS3Client s3;
    private final AmazonSQSClient sqs;

    public MessageAndBytes(String messageReceiptHandle, byte[] bytes, long timestamp, String s3Id,
            String queueName, String bucketName, AmazonS3Client s3, AmazonSQSClient sqs) {
        this.messageReceiptHandle = messageReceiptHandle;
        this.bytes = bytes;
        this.timestamp = timestamp;
        this.s3Id = s3Id;
        this.queueName = queueName;
        this.bucketName = bucketName;
        this.s3 = s3;
        this.sqs = sqs;
    }

    public byte[] bytes() {
        return bytes;
    }

    public long lastModifiedTime() {
        return timestamp;
    }

    public void deleteMessage(AmazonS3Client s3, AmazonSQSClient sqs) {
        s3.deleteObject(bucketName, s3Id);
        sqs.deleteMessage(new DeleteMessageRequest(queueName, messageReceiptHandle));
    }

    public void deleteMessage() {
        deleteMessage(s3, sqs);
    }

    @Override
    public String toString() {
        return "MessageAndBytes [messageReceiptHandle=" + messageReceiptHandle + ", bytes="
                + Arrays.toString(bytes) + ", timestamp=" + timestamp + ", s3Id=" + s3Id
                + ", bucketName=" + bucketName + ", queueName=" + queueName + ", s3=" + s3
                + ", sqs=" + sqs + "]";
    }

}