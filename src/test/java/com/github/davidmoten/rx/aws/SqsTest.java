package com.github.davidmoten.rx.aws;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.util.StringInputStream;
import com.github.davidmoten.junit.Asserts;

import io.reactivex.exceptions.CompositeException;
import io.reactivex.schedulers.TestScheduler;
import io.reactivex.subscribers.TestSubscriber;

public final class SqsTest {

    @Test(timeout = 5000)
    public void testFirstCallToReceiveMessagesReturnsOneMessage() {
        final AmazonSQSClient sqs = Mockito.mock(AmazonSQSClient.class);
        final String queueName = "queue";
        Mockito.when(sqs.getQueueUrl(queueName)).thenAnswer(x -> new GetQueueUrlResult().withQueueUrl(queueName));
        Mockito.when(sqs.receiveMessage(Mockito.<ReceiveMessageRequest>any()))
                .thenReturn(new ReceiveMessageResult().withMessages(new Message().withBody("body1")));
        Sqs.queueName(queueName) //
                .sqsFactory(() -> sqs) //
                .messages() //
                .map(m -> m.message()) //
                .doOnError(Throwable::printStackTrace) //
                .take(1) //
                .test() //
                .awaitDone(10, TimeUnit.SECONDS) //
                .assertComplete() //
                .assertValue("body1");
        final InOrder inorder = Mockito.inOrder(sqs);
        inorder.verify(sqs, Mockito.atLeastOnce()).getQueueUrl(queueName);
        inorder.verify(sqs, Mockito.times(1)).receiveMessage(Mockito.<ReceiveMessageRequest>any());
        inorder.verify(sqs, Mockito.times(1)).shutdown();
        inorder.verifyNoMoreInteractions();
    }

    @Test(timeout = 5000)
    public void testFirstCallToReceiveMessagesReturnsOneMessageAndHonoursBackpressure() {
        final AmazonSQSClient sqs = Mockito.mock(AmazonSQSClient.class);
        final String queueName = "queue";
        Mockito.when(sqs.getQueueUrl(queueName)).thenAnswer(x -> new GetQueueUrlResult().withQueueUrl(queueName));
        Mockito.when(sqs.receiveMessage(Mockito.<ReceiveMessageRequest>any()))
                .thenReturn(new ReceiveMessageResult().withMessages(new Message().withBody("body1")));
        Sqs.queueName(queueName) //
                .sqsFactory(() -> sqs) //
                .messages() //
                .map(m -> m.message()) //
                .doOnError(Throwable::printStackTrace) //
                .test(0) //
                .requestMore(1) //
                .assertValue("body1")//
                .assertNotComplete() //
                .cancel();
        final InOrder inorder = Mockito.inOrder(sqs);
        inorder.verify(sqs, Mockito.atLeastOnce()).getQueueUrl(queueName);
        inorder.verify(sqs, Mockito.times(1)).receiveMessage(Mockito.<ReceiveMessageRequest>any());
        inorder.verify(sqs, Mockito.times(1)).shutdown();
        inorder.verifyNoMoreInteractions();
    }

    @Test(timeout = 5000)
    public void testFirstCallToReceiveMessagesReturnsNoMessagesThenSecondCallReturnsTwoMessages() {
        final AmazonSQSClient sqs = Mockito.mock(AmazonSQSClient.class);
        final String queueName = "queue";
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
                .test() //
                .awaitDone(10, TimeUnit.SECONDS) //
                .assertComplete() //
                .assertValues("body1", "body2");
        final InOrder inorder = Mockito.inOrder(sqs);
        inorder.verify(sqs, Mockito.atLeastOnce()).getQueueUrl(queueName);
        inorder.verify(sqs, Mockito.times(2)).receiveMessage(Mockito.<ReceiveMessageRequest>any());
        inorder.verify(sqs, Mockito.times(1)).shutdown();
        inorder.verifyNoMoreInteractions();
    }

    @Test(timeout = 5000)
    public void testFirstCallToReceiveMessagesReturnsOneViaS3() throws UnsupportedEncodingException {
        final AmazonSQSClient sqs = Mockito.mock(AmazonSQSClient.class);
        final AmazonS3Client s3 = Mockito.mock(AmazonS3Client.class);
        final String queueName = "queue";
        final String s3Id = "123";
        Mockito.when(sqs.getQueueUrl(queueName)).thenAnswer(x -> new GetQueueUrlResult().withQueueUrl(queueName));
        final String receiptHandle = "abc";
        Mockito.when(sqs.receiveMessage(Mockito.<ReceiveMessageRequest>any())).thenReturn(
                new ReceiveMessageResult().withMessages(new Message().withReceiptHandle(receiptHandle).withBody(s3Id)));
        final String bucketName = "bucket";
        Mockito.when(s3.doesObjectExist(bucketName, s3Id)).thenAnswer(x -> true);
        final S3Object s3Object = mock(S3Object.class);
        Mockito.when(s3Object.getObjectContent())
                .thenReturn(new S3ObjectInputStream(new StringInputStream("body1"), null));
        final ObjectMetadata om = new ObjectMetadata();
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
                .test() //
                .awaitDone(10, TimeUnit.SECONDS) //
                .assertComplete() //
                .assertValues("body1");
        final InOrder inorder = Mockito.inOrder(sqs, s3, s3Object);
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

    @Test(timeout = 50000000)
    public void testFirstCallToReceiveMessagesReturnsOneWithNoS3ObjectAndOneWithS3Object()
            throws UnsupportedEncodingException {
        final AmazonSQSClient sqs = Mockito.mock(AmazonSQSClient.class);
        final AmazonS3Client s3 = Mockito.mock(AmazonS3Client.class);
        final String queueName = "queue";
        final String s3Id = "123";
        final String s3Id2 = "124";
        Mockito.when(sqs.getQueueUrl(queueName)).thenAnswer(x -> new GetQueueUrlResult().withQueueUrl(queueName));
        final String receiptHandle = "abc";
        final String receiptHandle2 = "abc2";
        Mockito.when(sqs.receiveMessage(Mockito.<ReceiveMessageRequest>any()))
                .thenReturn(new ReceiveMessageResult()
                        .withMessages(new Message().withReceiptHandle(receiptHandle).withBody(s3Id)))
                .thenReturn(new ReceiveMessageResult()
                        .withMessages(new Message().withReceiptHandle(receiptHandle2).withBody(s3Id2)));
        final String bucketName = "bucket";
        Mockito.when(s3.doesObjectExist(bucketName, s3Id)).thenReturn(false);
        Mockito.when(s3.doesObjectExist(bucketName, s3Id2)).thenReturn(true);
        final S3Object s3Object = mock(S3Object.class);
        Mockito.when(s3Object.getObjectContent())
                .thenReturn(new S3ObjectInputStream(new StringInputStream("body2"), null));
        final ObjectMetadata om = new ObjectMetadata();
        om.setLastModified(new Date(1001));
        Mockito.when(s3Object.getObjectMetadata()).thenReturn(om);
        Mockito.when(s3.getObject(bucketName, s3Id2)).thenReturn(s3Object);
        Sqs.queueName(queueName) //
                .sqsFactory(() -> sqs) //
                .bucketName("bucket") //
                .s3Factory(() -> s3) //
                .messages() //
                .doOnNext(SqsMessage::deleteMessage) //
                .map(m -> m.message()) //
                .doOnError(Throwable::printStackTrace) //
                .take(1) //
                .test() //
                .awaitDone(10, TimeUnit.SECONDS) //
                .assertComplete() //
                .assertValues("body2");
        final InOrder inorder = Mockito.inOrder(sqs, s3, s3Object);
        inorder.verify(sqs, Mockito.atLeastOnce()).getQueueUrl(queueName);
        inorder.verify(sqs, Mockito.times(1)).receiveMessage(Mockito.<ReceiveMessageRequest>any());
        inorder.verify(s3, Mockito.times(1)).doesObjectExist(bucketName, s3Id);
        inorder.verify(sqs, Mockito.times(1)).deleteMessage(queueName, receiptHandle);
        inorder.verify(sqs, Mockito.times(1)).receiveMessage(Mockito.<ReceiveMessageRequest>any());
        inorder.verify(s3, Mockito.times(1)).doesObjectExist(bucketName, s3Id2);
        inorder.verify(s3, Mockito.times(1)).getObject(bucketName, s3Id2);
        inorder.verify(s3Object, Mockito.times(1)).getObjectContent();
        inorder.verify(s3Object, Mockito.times(1)).getObjectMetadata();
        inorder.verify(s3, Mockito.times(1)).deleteObject(bucketName, s3Id2);
        inorder.verify(sqs, Mockito.times(1)).deleteMessage(queueName, receiptHandle2);
        inorder.verify(sqs, Mockito.times(1)).shutdown();
        inorder.verify(s3, Mockito.times(1)).shutdown();
        inorder.verifyNoMoreInteractions();
    }

    @Test
    public void isUtilityClass() {
        Asserts.assertIsUtilityClass(Sqs.class);
    }

    @Test
    public void testReadAndCloseWhenException() throws IOException {
        IOException e = null;
        try {
            final InputStream is = Mockito.mock(InputStream.class);
            e = new IOException();
            Mockito.when(is.read(Mockito.any(byte[].class))).thenThrow(e);
            Sqs.readAndClose(is);
        } catch (final RuntimeException ex) {
            assertEquals(e, ex.getCause());
        }
    }

    @Test(expected = NullPointerException.class)
    public void testBucketNameAndS3FactoryMustBothBeSpecified() {
        Sqs.queueName("queue").sqsFactory(() -> AmazonSQSClientBuilder.defaultClient()).bucketName(null)
                .s3Factory(() -> AmazonS3ClientBuilder.defaultClient()).messages();
    }

    @Test(timeout = 5000)
    public void testPollingReturnsAllAvailableMessagesAtEachScheduledCall() {
        final TestScheduler sched = new TestScheduler();
        final AmazonSQSClient sqs = Mockito.mock(AmazonSQSClient.class);
        final String queueName = "queue";
        Mockito.when(sqs.getQueueUrl(queueName)).thenAnswer(x -> new GetQueueUrlResult().withQueueUrl(queueName));
        Mockito.when(sqs.receiveMessage(Mockito.<ReceiveMessageRequest>any())) //
                .thenReturn(new ReceiveMessageResult()) //
                .thenReturn(new ReceiveMessageResult().withMessages(new Message().withBody("body1"))) //
                .thenReturn(new ReceiveMessageResult().withMessages(new Message().withBody("body2"))) //
                .thenReturn(new ReceiveMessageResult()) //
                .thenReturn(new ReceiveMessageResult().withMessages(new Message().withBody("body3"))) //
                .thenReturn(new ReceiveMessageResult()) //
                .thenReturn(new ReceiveMessageResult());
        final TestSubscriber<String> ts = Sqs.queueName(queueName) //
                .sqsFactory(() -> sqs) //
                .interval(1, TimeUnit.MINUTES, sched) //
                .messages() //
                .map(m -> m.message()) //
                .doOnError(Throwable::printStackTrace) //
                .test() //
                .assertNoValues() //
                .assertNotTerminated();

        sched.advanceTimeBy(1, TimeUnit.MINUTES);
        ts.assertValues("body1", "body2") //
                .assertNotTerminated();
        sched.advanceTimeBy(1, TimeUnit.MINUTES);
        ts.assertValues("body1", "body2", "body3") //
                .assertNotTerminated();
        sched.advanceTimeBy(1, TimeUnit.MINUTES);
        ts.assertValueCount(3) //
                .assertNotTerminated() //
                .cancel();
        final InOrder inorder = Mockito.inOrder(sqs);
        inorder.verify(sqs, Mockito.atLeastOnce()).getQueueUrl(queueName);
        // TODO why times(1), should be times(6)?
        inorder.verify(sqs, Mockito.times(1)).receiveMessage(Mockito.<ReceiveMessageRequest>any());
        inorder.verify(sqs, Mockito.times(1)).shutdown();
        inorder.verifyNoMoreInteractions();
    }

    @Test
    public void testSendMessage() {
        final AmazonSQSClient sqs = Mockito.mock(AmazonSQSClient.class);
        final AmazonS3Client s3 = Mockito.mock(AmazonS3Client.class);
        Sqs.sendToQueueUsingS3(sqs, "queueUrl", s3, "bucket", new byte[] { 1, 2 });
    }

    @SuppressWarnings("unchecked")
    @Test
    public void ensureIfSendToSqsFailsThatS3ObjectIsDeleted() {
        final AmazonSQSClient sqs = Mockito.mock(AmazonSQSClient.class);
        final AmazonS3Client s3 = Mockito.mock(AmazonS3Client.class);
        Mockito.when(sqs.sendMessage(Mockito.anyString(), Mockito.anyString())).thenThrow(RuntimeException.class);
        try {
            Sqs.sendToQueueUsingS3(sqs, "queueUrl", s3, "bucket", new byte[] { 1, 2 });
        } catch (final RuntimeException e) {
            assertTrue(e instanceof CompositeException);
            final InOrder inorder = Mockito.inOrder(sqs, s3);
            inorder.verify(s3, Mockito.times(1)).putObject(Mockito.anyString(), Mockito.anyString(), Mockito.any(),
                    Mockito.any());
            inorder.verify(sqs, Mockito.times(1)).sendMessage(Mockito.anyString(), Mockito.anyString());
            inorder.verify(s3, Mockito.times(1)).deleteObject(Mockito.anyString(), Mockito.anyString());
            inorder.verifyNoMoreInteractions();
        }
    }
    // @SuppressWarnings("unused")
    // public static void main(String[] args) {
    // ClientConfiguration cc;
    // if (false)
    // cc = new
    // ClientConfiguration().withProxyHost("proxy.amsa.gov.au").withProxyPort(8080);
    // else
    // cc = new ClientConfiguration();
    // AWSCredentialsProvider credentials = new
    // SystemPropertiesCredentialsProvider();
    // Func0<AmazonSQSClient> sqs = () -> new AmazonSQSClient(credentials, cc)
    // .withRegion(Region.getRegion(Regions.AP_SOUTHEAST_2));
    // Func0<AmazonS3Client> s3 = () -> new AmazonS3Client(credentials, cc)
    // .withRegion(Region.getRegion(Regions.AP_SOUTHEAST_2));
    // String bucketName = "cts-gateway-requests";
    // String queueName = bucketName;
    // Sqs.queueName(queueName) //
    // .sqsFactory(sqs) //
    // .bucketName(bucketName) //
    // .s3Factory(s3) //
    // .messages() //
    // .subscribeOn(Schedulers.io()) //
    // .doOnNext(System.out::println) //
    // .doOnNext(SqsMessage::deleteMessage) //
    // .doOnError(e -> {
    // e.printStackTrace();
    // System.out.println(Thread.currentThread().getName());
    // }) //
    // .retryWhen(RetryWhen.delay(5, TimeUnit.SECONDS).build(), Schedulers.io())
    // //
    // .toBlocking().subscribe();
    // }

}
