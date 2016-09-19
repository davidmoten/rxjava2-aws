package com.github.davidmoten.rx.aws;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Optional;

import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageResult;
import com.github.davidmoten.rx.aws.SqsMessage.Client;

public class SqsMessageTest {

	@Test
	public void test() {
		AmazonSQSClient sqs = Mockito.mock(AmazonSQSClient.class);
		SqsMessage m = createMessage(sqs);
		assertEquals(1000L, m.lastModifiedTime());
		assertTrue(m.toString().contains("123"));
	}

	private static SqsMessage createMessage(AmazonSQSClient sqs) {
		return new SqsMessage("r", new byte[] {}, 1000, Optional.of("123"),
				new SqsMessage.Service(Optional.empty(), () -> sqs, Optional.empty(), sqs, "queue", Optional.empty()));
	}

	@Test
	public void testClientFromFactory() {
		AmazonSQSClient sqs = Mockito.mock(AmazonSQSClient.class);
		SqsMessage m = new SqsMessage("receiptHandle", new byte[] {}, 1000, Optional.empty(),
				new SqsMessage.Service(Optional.empty(), () -> sqs, Optional.empty(), sqs, "queue", Optional.empty()));
		Mockito.when(sqs.deleteMessage("queue", "receiptHandle")).thenReturn(new DeleteMessageResult());
		m.deleteMessage(Client.FROM_FACTORY);
		InOrder inorder = Mockito.inOrder(sqs);
		inorder.verify(sqs, Mockito.times(1)).deleteMessage("queue", "receiptHandle");
		inorder.verify(sqs, Mockito.times(1)).shutdown();
		Mockito.verifyNoMoreInteractions(sqs);
	}

	@Test
	public void testClientFromSourceFailsThenFailsOverToFromFactory() {
		AmazonSQSClient sqs = Mockito.mock(AmazonSQSClient.class);
		SqsMessage m = new SqsMessage("receiptHandle", new byte[] {}, 1000, Optional.empty(),
				new SqsMessage.Service(Optional.empty(), () -> sqs, Optional.empty(), sqs, "queue", Optional.empty()));
		Mockito.when(sqs.deleteMessage("queue", "receiptHandle")).thenThrow(new RuntimeException())
				.thenReturn(new DeleteMessageResult());
		m.deleteMessage();
		InOrder inorder = Mockito.inOrder(sqs);
		inorder.verify(sqs, Mockito.times(2)).deleteMessage("queue", "receiptHandle");
		inorder.verify(sqs, Mockito.times(1)).shutdown();
		Mockito.verifyNoMoreInteractions(sqs);
	}

}
