package com.github.davidmoten.rx.aws;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

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
import com.github.davidmoten.rx.aws.SqsMessageViaS3.Service;

import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Subscriber;
import rx.functions.Func0;
import rx.schedulers.Schedulers;

public final class Sqs {

	private Sqs() {
		// prevent instantiation
	}

	public static Observable<SqsMessageViaS3> messagesViaS3(Func0<AmazonS3Client> s3ClientFactory,
			Func0<AmazonSQSClient> sqsClientFactory, String queueName, String bucketName) {
		return Observable.using(sqsClientFactory,
				sqs -> createObservable(sqs, s3ClientFactory, sqsClientFactory, queueName, bucketName),
				sqs -> sqs.shutdown());
	}
	
	private static Observable<SqsMessageViaS3> createObservable(AmazonSQSClient sqs,
			Func0<AmazonS3Client> s3ClientFactory, Func0<AmazonSQSClient> sqsClientFactory, String queueName,
			String bucketName) {

		return Observable.using(s3ClientFactory, //
				s3 -> Observable.create(new OnSubscribe<SqsMessageViaS3>() {

					final Service service = new Service(s3ClientFactory, sqsClientFactory, s3, sqs, queueName,
							bucketName);

					@Override
					public void call(Subscriber<? super SqsMessageViaS3> subscriber) {
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
								String s3Id = message.getBody();
								if (!s3.doesObjectExist(bucketName, s3Id)) {
									sqs.deleteMessage(new DeleteMessageRequest(queueUrl, message.getReceiptHandle()));
								} else {
									S3Object object = s3.getObject(bucketName, s3Id);
									byte[] content = readAndClose(object.getObjectContent());

									long timestamp = object.getObjectMetadata().getLastModified().getTime();
									SqsMessageViaS3 mb = new SqsMessageViaS3(message.getReceiptHandle(), content,
											timestamp, s3Id, service);
									if (subscriber.isUnsubscribed()) {
										return;
									}
									subscriber.onNext(mb);
								}
							}
						}
					}
				}).onBackpressureBuffer(), s3 -> s3.shutdown());

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
		messagesViaS3(s3, sqs, "cts-gateway-requests", "cts-gateway-requests") //
				.subscribeOn(Schedulers.io()) //
				.doOnNext(SqsMessageViaS3::deleteMessage) //
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
