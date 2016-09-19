package com.github.davidmoten.rx.aws;

import static com.github.davidmoten.rx.testing.TestSubscriber2.subscribe;
import static org.mockito.Mockito.mock;

import java.io.UnsupportedEncodingException;
import java.util.Date;

import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.util.StringInputStream;
import com.github.davidmoten.junit.Asserts;

public class SqsTest {

	@Test(timeout = 5000)
	public void testFirstCallToReceiveMessagesReturnsOneMessage() {
		AmazonSQSClient sqs = Mockito.mock(AmazonSQSClient.class);
		String queueName = "queue";
		Mockito.when(sqs.getQueueUrl(queueName)).thenAnswer(x -> new GetQueueUrlResult().withQueueUrl(queueName));
		Mockito.when(sqs.receiveMessage(Mockito.<ReceiveMessageRequest>any()))
				.thenReturn(new ReceiveMessageResult().withMessages(new Message().withBody("body1")));
		Sqs.queueName(queueName) //
				.sqsFactory(() -> sqs) //
				.messages() //
				.map(m -> m.message()) //
				.doOnError(Throwable::printStackTrace) //
				.take(1) //
				.to(subscribe()) //
				.awaitTerminalEvent() //
				.assertCompleted() //
				.assertValue("body1");
		InOrder inorder = Mockito.inOrder(sqs);
		inorder.verify(sqs, Mockito.atLeastOnce()).getQueueUrl(queueName);
		inorder.verify(sqs, Mockito.times(1)).receiveMessage(Mockito.<ReceiveMessageRequest>any());
		inorder.verify(sqs, Mockito.times(1)).shutdown();
		inorder.verifyNoMoreInteractions();
	}

	@Test(timeout = 5000)
	public void testFirstCallToReceiveMessagesReturnsOneMessageAndHonoursBackpressure() {
		AmazonSQSClient sqs = Mockito.mock(AmazonSQSClient.class);
		String queueName = "queue";
		Mockito.when(sqs.getQueueUrl(queueName)).thenAnswer(x -> new GetQueueUrlResult().withQueueUrl(queueName));
		Mockito.when(sqs.receiveMessage(Mockito.<ReceiveMessageRequest>any()))
				.thenReturn(new ReceiveMessageResult().withMessages(new Message().withBody("body1")));
		Sqs.queueName(queueName) //
				.sqsFactory(() -> sqs) //
				.messages() //
				.map(m -> m.message()) //
				.doOnError(Throwable::printStackTrace) //
				.to(subscribe(0)) //
				.requestMore(1) //
				.assertValue("body1")//
				.assertNotCompleted() //
				.unsubscribe();
		InOrder inorder = Mockito.inOrder(sqs);
		inorder.verify(sqs, Mockito.atLeastOnce()).getQueueUrl(queueName);
		inorder.verify(sqs, Mockito.times(1)).receiveMessage(Mockito.<ReceiveMessageRequest>any());
		inorder.verify(sqs, Mockito.times(1)).shutdown();
		inorder.verifyNoMoreInteractions();
	}

	@Test(timeout = 5000)
	public void testFirstCallToReceiveMessagesReturnsNoMessagesThenSecondCallReturnsTwoMessages() {
		AmazonSQSClient sqs = Mockito.mock(AmazonSQSClient.class);
		String queueName = "queue";
		Mockito.when(sqs.getQueueUrl(queueName)).thenAnswer(x -> new GetQueueUrlResult().withQueueUrl(queueName));
		Mockito.when(sqs.receiveMessage(Mockito.<ReceiveMessageRequest>any())).thenReturn(new ReceiveMessageResult())
				.thenReturn(new ReceiveMessageResult().withMessages(new Message().withBody("body1"),
						new Message().withBody("body2")));
		Sqs.queueName(queueName) //
				.sqsFactory(() -> sqs) //
				.messages() //
				.map(m -> m.message()) //
				.doOnError(Throwable::printStackTrace) //
				.take(2) //
				.to(subscribe()) //
				.awaitTerminalEvent() //
				.assertCompleted() //
				.assertValues("body1", "body2");
		InOrder inorder = Mockito.inOrder(sqs);
		inorder.verify(sqs, Mockito.atLeastOnce()).getQueueUrl(queueName);
		inorder.verify(sqs, Mockito.times(2)).receiveMessage(Mockito.<ReceiveMessageRequest>any());
		inorder.verify(sqs, Mockito.times(1)).shutdown();
		inorder.verifyNoMoreInteractions();
	}

	@Test(timeout = 5000)
	public void testFirstCallToReceiveMessagesReturnsOneViaS3() throws UnsupportedEncodingException {
		AmazonSQSClient sqs = Mockito.mock(AmazonSQSClient.class);
		AmazonS3Client s3 = Mockito.mock(AmazonS3Client.class);
		String queueName = "queue";
		String s3Id = "123";
		Mockito.when(sqs.getQueueUrl(queueName)).thenAnswer(x -> new GetQueueUrlResult().withQueueUrl(queueName));
		String receiptHandle = "abc";
		Mockito.when(sqs.receiveMessage(Mockito.<ReceiveMessageRequest>any())).thenReturn(
				new ReceiveMessageResult().withMessages(new Message().withReceiptHandle(receiptHandle).withBody(s3Id)));
		String bucketName = "bucket";
		Mockito.when(s3.doesObjectExist(bucketName, s3Id)).thenAnswer(x -> true);
		S3Object s3Object = mock(S3Object.class);
		Mockito.when(s3Object.getObjectContent())
				.thenReturn(new S3ObjectInputStream(new StringInputStream("body1"), null));
		ObjectMetadata om = new ObjectMetadata();
		om.setLastModified(new Date(1001));
		Mockito.when(s3Object.getObjectMetadata()).thenReturn(om);
		Mockito.when(s3.getObject(bucketName, s3Id)).thenReturn(s3Object);
		Sqs.queueName(queueName) //
				.sqsFactory(() -> sqs) //
				.bucketName("bucket") //
				.s3Factory(() -> s3) //
				.messages() //
				.doOnNext(SqsMessage::deleteMessage) //
				.map(m -> m.message()) //
				.doOnError(Throwable::printStackTrace) //
				.take(1) //
				.to(subscribe()) //
				.awaitTerminalEvent() //
				.assertCompleted() //
				.assertValues("body1");
		InOrder inorder = Mockito.inOrder(sqs, s3, s3Object);
		inorder.verify(sqs, Mockito.atLeastOnce()).getQueueUrl(queueName);
		inorder.verify(sqs, Mockito.times(1)).receiveMessage(Mockito.<ReceiveMessageRequest>any());
		inorder.verify(s3, Mockito.times(1)).doesObjectExist(bucketName, s3Id);
		inorder.verify(s3, Mockito.times(1)).getObject(bucketName, s3Id);
		inorder.verify(s3Object, Mockito.times(1)).getObjectContent();
		inorder.verify(s3Object, Mockito.times(1)).getObjectMetadata();
		inorder.verify(s3, Mockito.times(1)).deleteObject(bucketName, s3Id);
		inorder.verify(sqs, Mockito.times(1)).deleteMessage(queueName, receiptHandle);
		inorder.verify(sqs, Mockito.times(1)).shutdown();
		inorder.verify(s3, Mockito.times(1)).shutdown();
		inorder.verifyNoMoreInteractions();
	}

	@Test
	public void isUtilityClass() {
		Asserts.assertIsUtilityClass(Sqs.class);
	}

}
