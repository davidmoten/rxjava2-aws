package com.github.davidmoten.rx2.aws;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Optional;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageResult;
import com.github.davidmoten.rx2.aws.SqsMessage.Client;

public class SqsMessageTest {

    private static final String BUCKET = "bucket";
    private static final String RECEIPT_HANDLE = "receiptHandle";
    private static final String QUEUE = "queue";

    @Test
    public void test() {
        AmazonSQSClient sqs = Mockito.mock(AmazonSQSClient.class);
        SqsMessage m = createMessage(sqs);
        assertEquals(1000L, m.lastModifiedTime());
        assertTrue(m.toString().contains("123"));
    }

    private static SqsMessage createMessage(AmazonSQSClient sqs) {
        return new SqsMessage("r", new byte[] {}, 1000, Optional.of("123"), new SqsMessage.Service(
                Optional.empty(), () -> sqs, Optional.empty(), sqs, QUEUE, Optional.empty()));
    }

    @Test
    public void testClientFromFactory() {
        AmazonSQSClient sqs = Mockito.mock(AmazonSQSClient.class);
        SqsMessage m = new SqsMessage(RECEIPT_HANDLE, new byte[] {}, 1000, Optional.empty(),
                new SqsMessage.Service(Optional.empty(), () -> sqs, Optional.empty(), sqs, QUEUE,
                        Optional.empty()));
        Mockito.when(sqs.deleteMessage(QUEUE, RECEIPT_HANDLE))
                .thenReturn(new DeleteMessageResult());
        m.deleteMessage(Client.FROM_FACTORY);
        InOrder inorder = Mockito.inOrder(sqs);
        inorder.verify(sqs, Mockito.times(1)).deleteMessage(QUEUE, RECEIPT_HANDLE);
        inorder.verify(sqs, Mockito.times(1)).shutdown();
        Mockito.verifyNoMoreInteractions(sqs);
    }

    @Test
    public void testClientFromSourceFailsThenFailsOverToFromFactory() {
        AmazonSQSClient sqs = Mockito.mock(AmazonSQSClient.class);
        SqsMessage m = new SqsMessage(RECEIPT_HANDLE, new byte[] {}, 1000, Optional.empty(),
                new SqsMessage.Service(Optional.empty(), () -> sqs, Optional.empty(), sqs, QUEUE,
                        Optional.empty()));
        Mockito.when(sqs.deleteMessage(QUEUE, RECEIPT_HANDLE)).thenThrow(new RuntimeException())
                .thenReturn(new DeleteMessageResult());
        m.deleteMessage();
        InOrder inorder = Mockito.inOrder(sqs);
        inorder.verify(sqs, Mockito.times(2)).deleteMessage(QUEUE, RECEIPT_HANDLE);
        inorder.verify(sqs, Mockito.times(1)).shutdown();
        Mockito.verifyNoMoreInteractions(sqs);
    }

    @Test(expected = IllegalArgumentException.class)
    public void s3IdAndS3FactoryConflictThrowsException() {
        AmazonSQSClient sqs = Mockito.mock(AmazonSQSClient.class);
        AmazonS3Client s3 = Mockito.mock(AmazonS3Client.class);
        SqsMessage m = new SqsMessage(RECEIPT_HANDLE, new byte[] {}, 1000, Optional.empty(),
                new SqsMessage.Service(Optional.empty(), () -> sqs, Optional.empty(), sqs, QUEUE,
                        Optional.empty()));
        m.deleteMessage(Optional.of(s3), sqs);
    }

    @Test
    public void deleteMessageFromFactoryWhenS3FactoryExists() {
        AmazonSQSClient sqs = Mockito.mock(AmazonSQSClient.class);
        AmazonS3Client s3 = Mockito.mock(AmazonS3Client.class);
        String s3Id = "123";
        SqsMessage m = new SqsMessage(RECEIPT_HANDLE, new byte[] {}, 1000, Optional.of(s3Id),
                new SqsMessage.Service(Optional.of(() -> s3), () -> sqs, Optional.of(s3), sqs,
                        QUEUE, Optional.of(BUCKET)));
        Mockito.when(sqs.deleteMessage(QUEUE, RECEIPT_HANDLE))
                .thenReturn(new DeleteMessageResult());
        m.deleteMessage(Client.FROM_FACTORY);
        InOrder inorder = Mockito.inOrder(sqs, s3);
        inorder.verify(s3, Mockito.times(1)).deleteObject(BUCKET, s3Id);
        inorder.verify(sqs, Mockito.times(1)).deleteMessage(QUEUE, RECEIPT_HANDLE);
        inorder.verify(s3, Mockito.times(1)).shutdown();
        inorder.verify(sqs, Mockito.times(1)).shutdown();
        Mockito.verifyNoMoreInteractions(sqs, s3);
    }

    @Test
    public void shutdownDoesNotRethrowException() {
        AmazonSQSClient sqs = Mockito.mock(AmazonSQSClient.class);
        Mockito.doThrow(new RuntimeException()).when(sqs).shutdown();
        Util.shutdown(sqs);
    }

    @Test
    public void shutdownOfSqsAndS3FactoryCreatedClientsOccursWhenS3DeleteObjectFails() {
        AmazonSQSClient sqs = Mockito.mock(AmazonSQSClient.class);
        AmazonS3Client s3 = Mockito.mock(AmazonS3Client.class);
        String s3Id = "123";
        SqsMessage m = new SqsMessage(RECEIPT_HANDLE, new byte[] {}, 1000, Optional.of(s3Id),
                new SqsMessage.Service(Optional.of(() -> s3), () -> sqs, Optional.of(s3), sqs,
                        QUEUE, Optional.of(BUCKET)));
        Mockito.when(sqs.deleteMessage(QUEUE, RECEIPT_HANDLE))
                .thenReturn(new DeleteMessageResult());
        Mockito.doThrow(new RuntimeException()).when(s3).deleteObject(BUCKET, s3Id);
        try {
            m.deleteMessage(Client.FROM_FACTORY);
            Assert.fail();
        } catch (RuntimeException e) {
            // do nothing
        }
        InOrder inorder = Mockito.inOrder(sqs, s3);
        inorder.verify(s3, Mockito.times(1)).deleteObject(BUCKET, s3Id);
        inorder.verify(s3, Mockito.times(1)).shutdown();
        inorder.verify(sqs, Mockito.times(1)).shutdown();
        Mockito.verifyNoMoreInteractions(sqs, s3);
    }

}
