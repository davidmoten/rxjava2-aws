package com.github.davidmoten.rx.aws;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageResult;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.util.StringInputStream;

import rx.observers.TestSubscriber;
import rx.schedulers.Schedulers;

public class SqsTest {

	@Test
	@Ignore
	public void test() {
		TestSubscriber<Integer> ts = TestSubscriber.create();
		MySqsClient client = new MySqsClient();
		Sqs.queueName("queue") //
				.sqsFactory(() -> client) //
				.messages() //
				.map(m -> Integer.parseInt(m.message())) //
				.doOnError(Throwable::printStackTrace) //
				.take(12) //
				.subscribeOn(Schedulers.io()) //
				.subscribe(ts);
		ts.awaitTerminalEvent();
		ts.assertCompleted();
		assertEquals(IntStream.rangeClosed(1, 12).boxed().collect(Collectors.toList()), ts.getOnNextEvents());
	}

	@Test
	public void testFirstCallToReceiveMessagesReturnsOneMessage() {
		AmazonSQSClient sqs = Mockito.mock(AmazonSQSClient.class);
		String queueName = "queue";
		Mockito.when(sqs.getQueueUrl(queueName)).thenAnswer(x -> new GetQueueUrlResult().withQueueUrl(queueName));
		Mockito.when(sqs.receiveMessage(Mockito.<ReceiveMessageRequest>any()))
				.thenReturn(new ReceiveMessageResult().withMessages(new Message().withBody("body1")));
		TestSubscriber<String> ts = TestSubscriber.create();
		Sqs.queueName(queueName) //
				.sqsFactory(() -> sqs) //
				.messages() //
				.map(m -> m.message()) //
				.doOnError(Throwable::printStackTrace) //
				.take(1) //
				.subscribe(ts);
		ts.awaitTerminalEvent();
		ts.assertCompleted();
		ts.assertValue("body1");
		Mockito.verify(sqs, Mockito.atLeastOnce()).getQueueUrl(queueName);
		Mockito.verify(sqs, Mockito.times(1)).receiveMessage(Mockito.<ReceiveMessageRequest>any());
		Mockito.verify(sqs, Mockito.times(1)).shutdown();
		Mockito.verifyNoMoreInteractions(sqs);
	}

	@Test
	public void testFirstCallToReceiveMessagesReturnsOneMessageAndHonoursBackpressure() {
		AmazonSQSClient sqs = Mockito.mock(AmazonSQSClient.class);
		String queueName = "queue";
		Mockito.when(sqs.getQueueUrl(queueName)).thenAnswer(x -> new GetQueueUrlResult().withQueueUrl(queueName));
		Mockito.when(sqs.receiveMessage(Mockito.<ReceiveMessageRequest>any()))
				.thenReturn(new ReceiveMessageResult().withMessages(new Message().withBody("body1")));
		TestSubscriber<String> ts = TestSubscriber.create(0);
		Sqs.queueName(queueName) //
				.sqsFactory(() -> sqs) //
				.messages() //
				.map(m -> m.message()) //
				.doOnError(Throwable::printStackTrace) //
				.subscribe(ts);
		ts.requestMore(1);
		ts.assertValue("body1");
		ts.assertNotCompleted();
		ts.unsubscribe();
		Mockito.verify(sqs, Mockito.atLeastOnce()).getQueueUrl(queueName);
		Mockito.verify(sqs, Mockito.times(1)).receiveMessage(Mockito.<ReceiveMessageRequest>any());
		Mockito.verify(sqs, Mockito.times(1)).shutdown();
		Mockito.verifyNoMoreInteractions(sqs);
	}

	@Test
	public void testFirstCallToReceiveMessagesReturnsNoMessagesThenSecondCallReturnsTwoMessages() {
		AmazonSQSClient sqs = Mockito.mock(AmazonSQSClient.class);
		String queueName = "queue";
		Mockito.when(sqs.getQueueUrl(queueName)).thenAnswer(x -> new GetQueueUrlResult().withQueueUrl(queueName));
		Mockito.when(sqs.receiveMessage(Mockito.<ReceiveMessageRequest>any())).thenReturn(new ReceiveMessageResult())
				.thenReturn(new ReceiveMessageResult().withMessages(new Message().withBody("body1"),
						new Message().withBody("body2")));
		TestSubscriber<String> ts = TestSubscriber.create();
		Sqs.queueName(queueName) //
				.sqsFactory(() -> sqs) //
				.messages() //
				.map(m -> m.message()) //
				.doOnError(Throwable::printStackTrace) //
				.take(2) //
				.subscribe(ts);
		ts.awaitTerminalEvent();
		ts.assertCompleted();
		ts.assertValues("body1", "body2");
		Mockito.verify(sqs, Mockito.atLeastOnce()).getQueueUrl(queueName);
		Mockito.verify(sqs, Mockito.times(2)).receiveMessage(Mockito.<ReceiveMessageRequest>any());
		Mockito.verify(sqs, Mockito.times(1)).shutdown();
		Mockito.verifyNoMoreInteractions(sqs);
	}

	@Test(timeout = 5000000)
	public void testFirstCallToReceiveMessagesReturnsOneViaS3() throws UnsupportedEncodingException {
		AmazonSQSClient sqs = Mockito.mock(AmazonSQSClient.class);
		AmazonS3Client s3 = Mockito.mock(AmazonS3Client.class);
		String queueName = "queue";
		String s3Id = "123";
		Mockito.when(sqs.getQueueUrl(queueName)).thenAnswer(x -> new GetQueueUrlResult().withQueueUrl(queueName));
		Mockito.when(sqs.receiveMessage(Mockito.<ReceiveMessageRequest>any()))
				.thenReturn(new ReceiveMessageResult().withMessages(new Message().withBody(s3Id)));
		String bucketName = "bucket";
		Mockito.when(s3.doesObjectExist(bucketName, s3Id)).thenAnswer(x -> true);
		S3Object s3Object = mock(S3Object.class);
		Mockito.when(s3Object.getObjectContent())
				.thenReturn(new S3ObjectInputStream(new StringInputStream("body1"), null));
		ObjectMetadata om = new ObjectMetadata();
		om.setLastModified(new Date(1001));
		Mockito.when(s3Object.getObjectMetadata()).thenReturn(om);
		Mockito.when(s3.getObject(bucketName, s3Id)).thenReturn(s3Object);
		TestSubscriber<String> ts = TestSubscriber.create();
		Sqs.queueName(queueName) //
				.sqsFactory(() -> sqs) //
				.bucketName("bucket") //
				.s3Factory(() -> s3) //
				.messages() //
				.map(m -> m.message()) //
				.doOnError(Throwable::printStackTrace) //
				.take(1) //
				.subscribe(ts);
		ts.awaitTerminalEvent();
		ts.assertCompleted();
		ts.assertValues("body1");
		Mockito.verify(sqs, Mockito.atLeastOnce()).getQueueUrl(queueName);
		Mockito.verify(sqs, Mockito.times(1)).receiveMessage(Mockito.<ReceiveMessageRequest>any());
		Mockito.verify(s3, Mockito.times(1)).doesObjectExist(bucketName, s3Id);
		Mockito.verify(sqs, Mockito.times(1)).shutdown();
		Mockito.verify(s3, Mockito.times(1)).getObject(bucketName, s3Id);
		Mockito.verify(s3Object, Mockito.times(1)).getObjectContent();
		Mockito.verify(s3Object, Mockito.times(1)).getObjectMetadata();
		Mockito.verify(s3, Mockito.times(1)).shutdown();
		Mockito.verifyNoMoreInteractions(sqs, s3, s3Object);
	}

	public static class MySqsClient extends AmazonSQSClient {

		public MySqsClient() {
			super();
			System.out.println("created");
		}

		int count = 1;
		final Set<String> messages = new HashSet<String>();

		@Override
		public DeleteMessageResult deleteMessage(String queueUrl, String receiptHandle) {
			return new DeleteMessageResult();
		}

		@Override
		public GetQueueUrlResult getQueueUrl(String queueName) {
			System.out.println("getQueueUrl");
			return new GetQueueUrlResult().withQueueUrl(queueName);
		}

		@Override
		public ReceiveMessageResult receiveMessage(ReceiveMessageRequest receiveMessageRequest) {
			System.out.println("receiveMessage");
			try {
				int n = (int) Math.round(Math.random() * 3);

				List<Message> list = IntStream.range(1, n) //
						.mapToObj(i -> new Message() //
								.withBody(count++ + "") //
								.withReceiptHandle(count + "")) //
						.peek(m -> messages.add(m.getBody())) //
						.collect(Collectors.toList());
				Thread.sleep(Math.round(Math.random() * 1000));
				System.out.println("returning " + list.size() + " messages");
				return new ReceiveMessageResult().withMessages(list);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}

	}

}
