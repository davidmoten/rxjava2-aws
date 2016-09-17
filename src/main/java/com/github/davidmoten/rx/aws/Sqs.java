package com.github.davidmoten.rx.aws;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.github.davidmoten.rx.RetryWhen;
import com.github.davidmoten.rx.aws.SqsMessage.Service;
import com.github.davidmoten.util.Preconditions;

import rx.Observable;
import rx.Observer;
import rx.functions.Func0;
import rx.observables.SyncOnSubscribe;
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

		public ViaS3Builder bucketName(String bucketName) {
			this.bucketName = Optional.of(bucketName);
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

		public SqsBuilder s3Factory(Func0<AmazonS3Client> s3Factory) {
			sqsBuilder.s3 = Optional.of(s3Factory);
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

		return Observable.using(() -> s3ClientFactory.map(Func0::call), //
				s3 -> createObservable(sqs, s3ClientFactory, sqsClientFactory, queueName, bucketName, s3),
				s3 -> s3.ifPresent(AmazonS3Client::shutdown));
	}

	private static Observable<SqsMessage> createObservable(AmazonSQSClient sqs,
			Optional<Func0<AmazonS3Client>> s3ClientFactory, Func0<AmazonSQSClient> sqsClientFactory, String queueName,
			Optional<String> bucketName, Optional<AmazonS3Client> s3) {
		final Service service = new Service(s3ClientFactory, sqsClientFactory, s3, sqs, queueName, bucketName);
		return Observable.create(new SyncOnSubscribe<State, SqsMessage>() {

			private ReceiveMessageRequest request;
			private String queueUrl;

			@Override
			protected State generateState() {
				queueUrl = sqs.getQueueUrl(queueName).getQueueUrl();
				request = new ReceiveMessageRequest(queueUrl) //
						.withWaitTimeSeconds(20) //
						.withMaxNumberOfMessages(10);
				return new State(new LinkedList<>());
			}

			@Override
			protected State next(State state, Observer<? super SqsMessage> observer) {
				final Queue<Message> q = state.queue;
				final AtomicReference<SqsMessage> next = new AtomicReference<>();
				while (next.get() == null) {
					while (q.isEmpty()) {
						ReceiveMessageResult result = sqs.receiveMessage(request);
						q.addAll(result.getMessages());
					}
					getNextMessage(sqs, bucketName, s3, service, observer, q, next);
				}
				observer.onNext(next.get());
				return state;
			}

			private void getNextMessage(AmazonSQSClient sqs, Optional<String> bucketName, Optional<AmazonS3Client> s3,
					final Service service, Observer<? super SqsMessage> observer, Queue<Message> queue,
					AtomicReference<SqsMessage> next) {
				Message message = queue.poll();
				if (bucketName.isPresent()) {
					String s3Id = message.getBody();
					if (!s3.get().doesObjectExist(bucketName.get(), s3Id)) {
						sqs.deleteMessage(new DeleteMessageRequest(queueUrl, message.getReceiptHandle()));
					} else {
						S3Object object = s3.get().getObject(bucketName.get(), s3Id);
						byte[] content = readAndClose(object.getObjectContent());
						long timestamp = object.getObjectMetadata().getLastModified().getTime();
						SqsMessage mb = new SqsMessage(message.getReceiptHandle(), content, timestamp,
								Optional.of(s3Id), service);
						next.set(mb);
					}
				} else {
					SqsMessage mb = new SqsMessage(message.getReceiptHandle(),
							message.getBody().getBytes(StandardCharsets.UTF_8), System.currentTimeMillis(),
							Optional.empty(), service);
					next.set(mb);
				}
			}
		});
	}

	private static final class State {

		final Queue<Message> queue;

		public State(Queue<Message> queue) {
			this.queue = queue;
		}

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

	@SuppressWarnings("unused")
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
				.bucketName(bucketName) //
				.s3Factory(s3) //
				.messages() //
				.subscribeOn(Schedulers.io()) //
				.doOnNext(System.out::println) //
				.doOnNext(SqsMessage::deleteMessage) //
				.doOnError(e -> {
					e.printStackTrace();
					System.out.println(Thread.currentThread().getName());
				}) //
				.retryWhen(RetryWhen.delay(5, TimeUnit.SECONDS).build(), Schedulers.io()) //
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
