package com.github.davidmoten.rx.aws;

import java.util.Arrays;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.sqs.AmazonSQSClient;

import rx.functions.Func0;

public final class SqsMessageViaS3 {

	private final String messageReceiptHandle;
	private final byte[] bytes;
	private final long timestamp;
	private final String s3Id;
	private final Service service;

	SqsMessageViaS3(String messageReceiptHandle, byte[] bytes, long timestamp, String s3Id, Service service) {
		this.messageReceiptHandle = messageReceiptHandle;
		this.bytes = bytes;
		this.timestamp = timestamp;
		this.s3Id = s3Id;
		this.service = service;
	}

	public byte[] bytes() {
		return bytes;
	}

	public long lastModifiedTime() {
		return timestamp;
	}

	public void deleteMessage() {
		try {
			deleteMessage(Client.FROM_SOURCE);
		} catch (RuntimeException e) {
			deleteMessage(Client.FROM_FACTORY);
		}
	}

	public void deleteMessage(Client client) {
		if (client == Client.FROM_SOURCE) {
			deleteMessage(service.s3, service.sqs);
		} else {
			AmazonS3Client s3 = service.s3Factory.call();
			AmazonSQSClient sqs = service.sqsFactory.call();
			try {
				deleteMessage(s3, sqs);
			} finally {
				try {
					s3.shutdown();
				} catch (RuntimeException e) {
				}
				try {
					sqs.shutdown();
				} catch (RuntimeException e) {

				}
			}
		}
	}

	public void deleteMessage(AmazonS3Client s3, AmazonSQSClient sqs) {
		s3.deleteObject(service.bucketName, s3Id);
		sqs.deleteMessage(service.queueName, messageReceiptHandle);
	}

	@Override
	public String toString() {
		return "MessageAndBytes [messageReceiptHandle=" + messageReceiptHandle + ", bytes=" + Arrays.toString(bytes)
				+ ", timestamp=" + timestamp + ", s3Id=" + s3Id + ", bucketName=" + service.bucketName + ", queueName="
				+ service.queueName + "]";
	}

	public static enum Client {
		FROM_SOURCE, FROM_FACTORY;
	}

	static class Service {

		final Func0<AmazonS3Client> s3Factory;
		final Func0<AmazonSQSClient> sqsFactory;
		final AmazonS3Client s3;
		final AmazonSQSClient sqs;
		final String queueName;
		final String bucketName;

		Service(Func0<AmazonS3Client> s3Factory, Func0<AmazonSQSClient> sqsFactory, AmazonS3Client s3,
				AmazonSQSClient sqs, String queueName, String bucketName) {
			this.s3Factory = s3Factory;
			this.sqsFactory = sqsFactory;
			this.s3 = s3;
			this.sqs = sqs;
			this.queueName = queueName;
			this.bucketName = bucketName;
		}

	}

}