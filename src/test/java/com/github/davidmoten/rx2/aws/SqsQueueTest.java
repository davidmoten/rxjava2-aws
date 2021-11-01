package com.github.davidmoten.rx2.aws;

import static org.junit.Assert.assertEquals;

import java.util.Optional;

import org.junit.Test;
import org.mockito.Mockito;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;

public class SqsQueueTest {

    @Test
    public void testFromQueueName() {
        SqsQueue q = SqsQueue.fromQueueName("myqueue");
        AmazonSQSClient sqs = Mockito.mock(AmazonSQSClient.class);
        String queueUrl = "https://myqueue";
        Mockito.when(sqs.getQueueUrl(Mockito.any(GetQueueUrlRequest.class)))
                .thenReturn(new GetQueueUrlResult().withQueueUrl(queueUrl));
        String url = q.getQueueUrl(sqs);
        assertEquals(queueUrl, url);
    }

    @Test
    public void testFromQueueNameAndAccountId() {
        SqsQueue q = SqsQueue.fromQueueNameAndOwnerAccountId("myqueue", "abc123");
        AmazonSQSClient sqs = Mockito.mock(AmazonSQSClient.class);
        String queueUrl = "https://myqueue";
        Mockito.when(sqs.getQueueUrl(Mockito.any(GetQueueUrlRequest.class)))
                .thenReturn(new GetQueueUrlResult().withQueueUrl(queueUrl));
        String url = q.getQueueUrl(sqs);
        assertEquals(queueUrl, url);
    }

    @Test
    public void testFromQueueUrl() {
        String queueUrl = "https://myqueue";
        SqsQueue q = SqsQueue.fromQueueUrl(queueUrl);
        AmazonSQSClient sqs = Mockito.mock(AmazonSQSClient.class);
        String url = q.getQueueUrl(sqs);
        assertEquals(queueUrl, url);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorMandatoryArguments1() {
        new SqsQueue(Optional.empty(), Optional.empty(), Optional.empty());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorMandatoryArguments2() {
        new SqsQueue(Optional.empty(), Optional.of("blah"), Optional.empty());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorMandatoryArguments3() {
        new SqsQueue(Optional.of("myqueue"), Optional.empty(), Optional.of("https://myqueue"));
    }
    
    @Test
    public void testConstructorMandatoryArguments4() {
        new SqsQueue(Optional.of("myqueue"), Optional.of("abc123"), Optional.empty());
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testConstructorMandatoryArguments5() {
        new SqsQueue(Optional.empty(), Optional.of("blah"), Optional.of("https://myqueue"));
    }

}
