package com.github.davidmoten.rx.aws;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.github.davidmoten.rx.RetryWhen;
import com.github.davidmoten.rx.aws.SqsMessage.Service;
import com.github.davidmoten.util.Preconditions;

import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Subscriber;
import rx.functions.Func0;
import rx.schedulers.Schedulers;

public final class Sqs {

	private Sqs() {
		// prevent instantiation
	}

	public static final class SqsBuilder {
		private final String queueName;
		private Func0<AmazonSQSClient> sqs = null;
		private Optional<Func0<AmazonS3Client>> s3 = Optional.empty();
		private Optional<String> bucketName = Optional.empty();

		SqsBuilder(String queueName) {
			Preconditions.checkNotNull(queueName);
			this.queueName = queueName;
		}

		public ViaS3Builder s3Factory(Func0<AmazonS3Client> s3Factory) {
			this.s3 = Optional.of(s3Factory);
			return new ViaS3Builder(this);
		}

		public SqsBuilder sqsFactory(Func0<AmazonSQSClient> sqsFactory) {
			this.sqs = sqsFactory;
			return this;
		}

		public Observable<SqsMessage> messages() {
			Preconditions.checkNotNull(sqs, "client must not be null");
			return Sqs.messages(sqs, s3, queueName, bucketName);
		}

	}

	public static final class ViaS3Builder {

		private final SqsBuilder sqsBuilder;

		public ViaS3Builder(SqsBuilder sqsBuilder) {
			this.sqsBuilder = sqsBuilder;
		}

		public SqsBuilder bucketName(String bucketName) {
			sqsBuilder.bucketName = Optional.of(bucketName);
			return sqsBuilder;
		}

	}

	public static SqsBuilder queueName(String queueName) {
		return new SqsBuilder(queueName);
	}

	static Observable<SqsMessage> messages(Func0<AmazonSQSClient> sqsClientFactory,
			Optional<Func0<AmazonS3Client>> s3ClientFactory, String queueName, Optional<String> bucketName) {
		Preconditions.checkNotNull(sqsClientFactory);
		Preconditions.checkNotNull(s3ClientFactory);
		Preconditions.checkNotNull(queueName);
		Preconditions.checkNotNull(bucketName);
		Preconditions.checkArgument(!s3ClientFactory.isPresent() || bucketName.isPresent(),
				"bucketName must be specified if an s3ClientFactory is present");
		return Observable.using(sqsClientFactory,
				sqs -> createObservable(sqs, s3ClientFactory, sqsClientFactory, queueName, bucketName),
				sqs -> sqs.shutdown());
	}

	private static Observable<SqsMessage> createObservable(AmazonSQSClient sqs,
			Optional<Func0<AmazonS3Client>> s3ClientFactory, Func0<AmazonSQSClient> sqsClientFactory, String queueName,
			Optional<String> bucketName) {

		// TODO support backpressure properly
		
		return Observable.using(() -> s3ClientFactory.map(Func0::call), //
				s3 -> createObservable(sqs, s3ClientFactory, sqsClientFactory, queueName, bucketName, s3), s3 -> s3.ifPresent(AmazonS3Client::shutdown));

	}

	private static Observable<SqsMessage> createObservable(AmazonSQSClient sqs,
			Optional<Func0<AmazonS3Client>> s3ClientFactory, Func0<AmazonSQSClient> sqsClientFactory, String queueName,
			Optional<String> bucketName, Optional<AmazonS3Client> s3) {
		return Observable.create(new OnSubscribe<SqsMessage>() {

			final Service service = new Service(s3ClientFactory, sqsClientFactory, s3, sqs, queueName,
					bucketName);

			@Override
			public void call(Subscriber<? super SqsMessage> subscriber) {
				String queueUrl = sqs.getQueueUrl(new GetQueueUrlRequest(queueName)).getQueueUrl();
				ReceiveMessageRequest request = new ReceiveMessageRequest(queueUrl) //
						.withWaitTimeSeconds(20) //
						.withMaxNumberOfMessages(10);
				while (!subscriber.isUnsubscribed()) {
					ReceiveMessageResult result = sqs.receiveMessage(request);
					if (!subscriber.isUnsubscribed()) {
						return;
					}
					for (Message message : result.getMessages()) {
						if (subscriber.isUnsubscribed()) {
							return;
						}
						if (bucketName.isPresent()) {
							String s3Id = message.getBody();
							if (!s3.get().doesObjectExist(bucketName.get(), s3Id)) {
								sqs.deleteMessage(
										new DeleteMessageRequest(queueUrl, message.getReceiptHandle()));
							} else {
								S3Object object = s3.get().getObject(bucketName.get(), s3Id);
								byte[] content = readAndClose(object.getObjectContent());
								long timestamp = object.getObjectMetadata().getLastModified().getTime();
								SqsMessage mb = new SqsMessage(message.getReceiptHandle(), content, timestamp,
										Optional.of(s3Id), service);
							}
						} else {
							sqs.sendMessage(queueUrl, "");
							SqsMessage mb = new SqsMessage(message.getReceiptHandle(),
									message.getBody().getBytes(StandardCharsets.UTF_8),
									System.currentTimeMillis(), Optional.empty(), service);
							if (subscriber.isUnsubscribed()) {
								return;
							}
							subscriber.onNext(mb);
						}
					}
				}
			}
		}).onBackpressureBuffer();
	}

	private static byte[] readAndClose(InputStream is) {
		try (BufferedInputStream b = new BufferedInputStream(is)) {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			byte[] bytes = new byte[8192];
			int n;
			while ((n = b.read(bytes)) != -1) {
				bos.write(bytes, 0, n);
			}
			return bos.toByteArray();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public static void main(String[] args) {
		ClientConfiguration cc;
		if (false)
			cc = new ClientConfiguration().withProxyHost("proxy.amsa.gov.au").withProxyPort(8080);
		else
			cc = new ClientConfiguration();
		AWSCredentialsProvider credentials = new SystemPropertiesCredentialsProvider();
		Func0<AmazonSQSClient> sqs = () -> new AmazonSQSClient(credentials, cc)
				.withRegion(Region.getRegion(Regions.AP_SOUTHEAST_2));
		Func0<AmazonS3Client> s3 = () -> new AmazonS3Client(credentials, cc)
				.withRegion(Region.getRegion(Regions.AP_SOUTHEAST_2));
		String bucketName = "cts-gateway-requests";
		String queueName = bucketName;
		Sqs.queueName(queueName) //
				.sqsFactory(sqs) //
				.s3Factory(s3) //
				.bucketName(bucketName) //
				.messages() //
				.subscribeOn(Schedulers.io()) //
				.doOnNext(System.out::println) //
				.doOnNext(SqsMessage::deleteMessage) //
				.doOnError(Throwable::printStackTrace) //
				.retryWhen(RetryWhen.delay(5, TimeUnit.SECONDS).build()) //
				.toBlocking().subscribe();

		// String queueUrl = sqs.getQueueUrl(new
		// GetQueueUrlRequest("cts-gateway-requests"))
		// .getQueueUrl();
		// Schedulers.computation().createWorker().schedule(() ->
		// sqs.shutdown(), 3, TimeUnit.SECONDS);
		// System.out.println("requesting");
		// ReceiveMessageRequest request = new
		// ReceiveMessageRequest(queueUrl).withWaitTimeSeconds(20)
		// .withMaxNumberOfMessages(10);
		// sqs.receiveMessage(request);
		// System.out.println("finished");
	}

}
