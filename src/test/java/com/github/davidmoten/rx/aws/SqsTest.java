package com.github.davidmoten.rx.aws;

import static org.junit.Assert.assertEquals;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageResult;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;

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
		Mockito.when(sqs.receiveMessage(Mockito.<ReceiveMessageRequest>any()))
				.thenReturn(new ReceiveMessageResult())
				.thenReturn(new ReceiveMessageResult().withMessages(new Message().withBody("body1"),new Message().withBody("body2")));
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
