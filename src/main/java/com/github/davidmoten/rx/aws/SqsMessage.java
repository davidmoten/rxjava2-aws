package com.github.davidmoten.rx.aws;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.github.davidmoten.util.Preconditions;

import rx.functions.Func0;

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
		} catch (RuntimeException e) {
			deleteMessage(Client.FROM_FACTORY);
		}
	}

	public void deleteMessage(Client client) {
		if (client == Client.FROM_SOURCE) {
			deleteMessage(service.s3, service.sqs);
		} else {
			Optional<AmazonS3Client> s3 = service.s3Factory.map(Func0::call);
			AmazonSQSClient sqs = service.sqsFactory.call();
			try {
				deleteMessage(s3, sqs);
			} finally {
				if (s3.isPresent()) {
					try {
						s3.get().shutdown();
					} catch (RuntimeException e) {
					}
				}
				try {
					sqs.shutdown();
				} catch (RuntimeException e) {

				}
			}
		}
	}

	public void deleteMessage(Optional<AmazonS3Client> s3, AmazonSQSClient sqs) {
		Preconditions.checkArgument(!s3.isPresent() || s3Id.isPresent(), "s3Id must be present");
		if (s3Id.isPresent()) {
			s3.get().deleteObject(service.bucketName.get(), s3Id.get());
		}
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

		final Func0<AmazonSQSClient> sqsFactory;
		final Optional<Func0<AmazonS3Client>> s3Factory;
		final Optional<AmazonS3Client> s3;
		final AmazonSQSClient sqs;
		final String queueName;
		final Optional<String> bucketName;

		Service(Optional<Func0<AmazonS3Client>> s3Factory, Func0<AmazonSQSClient> sqsFactory, Optional<AmazonS3Client> s3,
				AmazonSQSClient sqs, String queueName, Optional<String> bucketName) {
			Preconditions.checkNotNull(s3Factory);
			Preconditions.checkNotNull(sqsFactory);
			Preconditions.checkNotNull(s3);
			Preconditions.checkNotNull(sqs);
			Preconditions.checkNotNull(queueName);
			Preconditions.checkNotNull(bucketName);
			this.s3Factory = s3Factory;
			this.sqsFactory = sqsFactory;
			this.s3 = s3;
			this.sqs = sqs;
			this.queueName = queueName;
			this.bucketName = bucketName;
		}

	}

}