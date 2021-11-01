package com.github.davidmoten.rx2.aws;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.junit.Assert;
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
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.util.StringInputStream;
import com.github.davidmoten.junit.Asserts;
import com.github.davidmoten.rx2.aws.Sqs.SqsBuilder;

import io.reactivex.exceptions.CompositeException;
import io.reactivex.schedulers.TestScheduler;
import io.reactivex.subscribers.TestSubscriber;

public final class SqsTest {

    @Test(timeout = 5000)
    public void testFirstCallToReceiveMessagesReturnsOneMessage() {
        final String queueName = "queue";
        SqsBuilder builder = Sqs.queueName(queueName);
        checkFirstCall(queueName, builder);
    }
    
    @Test(timeout = 5000)
    public void testFirstCallToReceiveMessagesReturnsOneMessageUsesAccountId() {
        final String queueName = "queue";
        SqsBuilder builder = Sqs.ownerAccountId("abc123").queueName(queueName);
        checkFirstCall(queueName, builder);
    }

    private void checkFirstCall(final String queueName, SqsBuilder builder) {
        final AmazonSQSClient sqs = Mockito.mock(AmazonSQSClient.class);
        Mockito.when(sqs.getQueueUrl(Mockito.<GetQueueUrlRequest>any()))
                .thenAnswer(x -> new GetQueueUrlResult().withQueueUrl(queueName));
        Mockito.when(sqs.receiveMessage(Mockito.<ReceiveMessageRequest>any())).thenReturn(
                new ReceiveMessageResult().withMessages(new Message().withBody("body1")));
        List<String> list = new CopyOnWriteArrayList<>();
        Consumer<String> logger = new Consumer<String>() {
            @Override
            public void accept(String msg) {
                list.add(msg);
            }
        };
        List<String> events = new ArrayList<>();
        builder.sqsFactory(() -> sqs) //
                .logger(logger).prePoll(() -> events.add("prePoll")) //
                .postPoll(e -> events.add("postPoll")) //
                .messages() //
                .map(m -> m.message()) //
                .doOnError(Throwable::printStackTrace) //
                .take(1) //
                .test() //
                .awaitDone(10, TimeUnit.SECONDS) //
                .assertComplete() //
                .assertValue("body1");
        final InOrder inorder = Mockito.inOrder(sqs);
        inorder.verify(sqs, Mockito.atLeastOnce()).getQueueUrl(Mockito.<GetQueueUrlRequest>any());
        inorder.verify(sqs, Mockito.times(1)).receiveMessage(Mockito.<ReceiveMessageRequest>any());
        inorder.verify(sqs, Mockito.times(1)).shutdown();
        inorder.verifyNoMoreInteractions();
        assertEquals(Arrays.asList("long polling for messages on queue=queue"), list);
        assertEquals(Arrays.asList("prePoll", "postPoll"), events);
    }
    
    @Test
    public void testFirstCallWithQueueUrl() {
        final AmazonSQSClient sqs = Mockito.mock(AmazonSQSClient.class);
        Mockito.when(sqs //
                .receiveMessage(Mockito.<ReceiveMessageRequest>any())) //
                .thenReturn(
                        new ReceiveMessageResult().withMessages(new Message().withBody("body1")));
        List<String> list = new CopyOnWriteArrayList<>();
        Consumer<String> logger = new Consumer<String>() {
            @Override
            public void accept(String msg) {
                list.add(msg);
            }
        };
        List<String> events = new ArrayList<>();
        Sqs.queueUrl("https://myqueue") //
                .sqsFactory(() -> sqs) //
                .logger(logger) //
                .prePoll(() -> events.add("prePoll")) //
                .postPoll(e -> events.add("postPoll")) //
                .messages() //
                .map(m -> m.message()) //
                .doOnError(Throwable::printStackTrace) //
                .take(1) //
                .test() //
                .awaitDone(10, TimeUnit.SECONDS) //
                .assertComplete() //
                .assertValue("body1");
        final InOrder inorder = Mockito.inOrder(sqs);
        inorder.verify(sqs, Mockito.times(1)).receiveMessage(Mockito.<ReceiveMessageRequest>any());
        inorder.verify(sqs, Mockito.times(1)).shutdown();
        inorder.verifyNoMoreInteractions();
        assertEquals(Arrays.asList("long polling for messages on queue=https://myqueue"), list);
        assertEquals(Arrays.asList("prePoll", "postPoll"), events);
    }

    @Test(timeout = 5000)
    public void testFirstCallToReceiveMessagesReturnsOneMessageAndHonoursBackpressure() {
        final AmazonSQSClient sqs = Mockito.mock(AmazonSQSClient.class);
        final String queueName = "queue";
        Mockito.when(sqs.getQueueUrl(Mockito.<GetQueueUrlRequest>any()))
                .thenAnswer(x -> new GetQueueUrlResult().withQueueUrl(queueName));
        Mockito.when(sqs.receiveMessage(Mockito.<ReceiveMessageRequest>any())).thenReturn(
                new ReceiveMessageResult().withMessages(new Message().withBody("body1")));
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
        inorder.verify(sqs, Mockito.atLeastOnce()).getQueueUrl(Mockito.<GetQueueUrlRequest>any());
        inorder.verify(sqs, Mockito.times(1)).receiveMessage(Mockito.<ReceiveMessageRequest>any());
        inorder.verify(sqs, Mockito.times(1)).shutdown();
        inorder.verifyNoMoreInteractions();
    }

    @Test(timeout = 5000)
    public void testFirstCallToReceiveMessagesReturnsNoMessagesThenSecondCallReturnsTwoMessages() {
        final AmazonSQSClient sqs = Mockito.mock(AmazonSQSClient.class);
        final String queueName = "queue";
        Mockito.when(sqs.getQueueUrl(Mockito.<GetQueueUrlRequest>any()))
                .thenAnswer(x -> new GetQueueUrlResult().withQueueUrl(queueName));
        Mockito.when(sqs.receiveMessage(Mockito.<ReceiveMessageRequest>any()))
                .thenReturn(new ReceiveMessageResult())
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
        inorder.verify(sqs, Mockito.atLeastOnce()).getQueueUrl(Mockito.<GetQueueUrlRequest>any());
        inorder.verify(sqs, Mockito.times(2)).receiveMessage(Mockito.<ReceiveMessageRequest>any());
        inorder.verify(sqs, Mockito.times(1)).shutdown();
        inorder.verifyNoMoreInteractions();
    }

    @Test(timeout = 5000)
    public void testFirstCallToReceiveMessagesReturnsOneViaS3()
            throws UnsupportedEncodingException {
        final AmazonSQSClient sqs = Mockito.mock(AmazonSQSClient.class);
        final AmazonS3Client s3 = Mockito.mock(AmazonS3Client.class);
        final String queueName = "queue";
        final String s3Id = "123";
        Mockito.when(sqs.getQueueUrl(Mockito.<GetQueueUrlRequest>any()))
                .thenAnswer(x -> new GetQueueUrlResult().withQueueUrl(queueName));
        final String receiptHandle = "abc";
        Mockito.when(sqs.receiveMessage(Mockito.<ReceiveMessageRequest>any()))
                .thenReturn(new ReceiveMessageResult().withMessages(
                        new Message().withReceiptHandle(receiptHandle).withBody(s3Id)));
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
        inorder.verify(sqs, Mockito.atLeastOnce()).getQueueUrl(Mockito.<GetQueueUrlRequest>any());
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
        Mockito.when(sqs.getQueueUrl(Mockito.<GetQueueUrlRequest>any()))
                .thenAnswer(x -> new GetQueueUrlResult().withQueueUrl(queueName));
        final String receiptHandle = "abc";
        final String receiptHandle2 = "abc2";
        Mockito.when(sqs.receiveMessage(Mockito.<ReceiveMessageRequest>any()))
                .thenReturn(new ReceiveMessageResult().withMessages(
                        new Message().withReceiptHandle(receiptHandle).withBody(s3Id)))
                .thenReturn(new ReceiveMessageResult().withMessages(
                        new Message().withReceiptHandle(receiptHandle2).withBody(s3Id2)));
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
        inorder.verify(sqs, Mockito.atLeastOnce()).getQueueUrl(Mockito.<GetQueueUrlRequest>any());
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
        Sqs.queueName("queue").sqsFactory(() -> AmazonSQSClientBuilder.defaultClient())
                .bucketName(null).s3Factory(() -> AmazonS3ClientBuilder.defaultClient()).messages();
    }

    @Test(timeout = 5000)
    public void testPollingReturnsAllAvailableMessagesAtEachScheduledCall() {
        final TestScheduler sched = new TestScheduler();
        final AmazonSQSClient sqs = Mockito.mock(AmazonSQSClient.class);
        final String queueName = "queue";
        Mockito.when(sqs.getQueueUrl(Mockito.<GetQueueUrlRequest>any()))
                .thenAnswer(x -> new GetQueueUrlResult().withQueueUrl(queueName));
        Mockito.when(sqs.receiveMessage(Mockito.<ReceiveMessageRequest>any())) //
                .thenReturn(new ReceiveMessageResult()) //
                .thenReturn(
                        new ReceiveMessageResult().withMessages(new Message().withBody("body1"))) //
                .thenReturn(
                        new ReceiveMessageResult().withMessages(new Message().withBody("body2"))) //
                .thenReturn(new ReceiveMessageResult()) //
                .thenReturn(
                        new ReceiveMessageResult().withMessages(new Message().withBody("body3"))) //
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
        inorder.verify(sqs, Mockito.atLeastOnce()).getQueueUrl(Mockito.<GetQueueUrlRequest>any()); // TODO
                                                                                                   // why
                                                                                                   // times(1),
                                                                                                   // should
                                                                                                   // be
                                                                                                   // times(6)?
        inorder.verify(sqs, Mockito.times(7)).receiveMessage(Mockito.<ReceiveMessageRequest>any());
        inorder.verify(sqs, Mockito.times(1)).shutdown();
        inorder.verifyNoMoreInteractions();
    }

    @Test
    public void testSendMessage() {
        final AmazonSQSClient sqs = Mockito.mock(AmazonSQSClient.class);
        final AmazonS3Client s3 = Mockito.mock(AmazonS3Client.class);
        Sqs.sendToQueueUsingS3(sqs, "queueUrl", s3, "bucket", new byte[] {1, 2});
    }

    @Test
    public void ensureIfSendToSqsFailsThatS3ObjectIsDeleted() {
        final AmazonSQSClient sqs = Mockito.mock(AmazonSQSClient.class);
        final AmazonS3Client s3 = Mockito.mock(AmazonS3Client.class);
        Mockito.when(sqs.sendMessage(Mockito.anyString(), Mockito.anyString()))
                .thenThrow(RuntimeException.class);
        try {
            Sqs.sendToQueueUsingS3(sqs, "queueUrl", s3, "bucket", new byte[] {1, 2});
        } catch (final RuntimeException e) {
            assertTrue(e instanceof CompositeException);
            final InOrder inorder = Mockito.inOrder(sqs, s3);
            inorder.verify(s3, Mockito.times(1)).putObject(Mockito.anyString(), Mockito.anyString(),
                    Mockito.any(), Mockito.any());
            inorder.verify(sqs, Mockito.times(1)).sendMessage(Mockito.anyString(),
                    Mockito.anyString());
            inorder.verify(s3, Mockito.times(1)).deleteObject(Mockito.anyString(),
                    Mockito.anyString());
            inorder.verifyNoMoreInteractions();
        }
    }

    @Test
    public void testCanUseNewAwsBuildersInFactoryMethods() {
        Sqs.queueName("queue").sqsFactory(
                () -> AmazonSQSClientBuilder.standard().withRegion("ap-southeast-2").build());
    }

    @Test
    public void testPollEvents() {
        List<Message> list = new ArrayList<>();
        List<String> events = new ArrayList<>();
        List<Message> result = Sqs.messages(() -> list, () -> events.add("prePoll"), e -> events.add("postPoll"));
        assertTrue(result == list);
        assertEquals(Arrays.asList("prePoll", "postPoll"), events);
    }
    
    @Test
    public void testPollEventsIfPrePollThrows() {
        List<Message> list = new ArrayList<>();
        List<String> events = new ArrayList<>();
        try {
            Sqs.messages(() -> list, () -> {
                throw new RuntimeException();
            }, e -> events.add("postPoll"));
            Assert.fail();
        } catch (RuntimeException e) {
            // expected
            assertTrue(events.isEmpty());
        }
    }
    
    @Test
    public void testPollEventsIfSupplierThrows() {
        List<String> events = new ArrayList<>();
        try {
            Sqs.messages(() -> {
                throw new RuntimeException();
            }, () -> events.add("prePoll"), //
                    e -> events.add(e.get().getClass().getSimpleName()));
            Assert.fail();
        } catch (RuntimeException e) {
            // expected
            assertEquals(Arrays.asList("prePoll", "RuntimeException"), events);
        }
    }
    
    @Test
    public void testPollEventsIfPostPollThrows() {
        List<String> events = new ArrayList<>();
        try {
            Sqs.messages(() -> new ArrayList<>(), //
                    () -> events.add("prePoll"), //
                    e -> {
                        throw new IllegalArgumentException();
                    });
            Assert.fail();
        } catch (IllegalArgumentException e) {
            // expected
            assertEquals(Arrays.asList("prePoll"), events);
        }
    }
    
    @Test
    public void testUncheckedCall() {
        assertEquals(1, (int) Sqs.uncheckedCall(() -> 1));   
    }
    
    @Test
    public void testUncheckedCallThrowsRuntimeException() {
        try {
            Sqs.uncheckedCall(() -> {
                throw new IllegalArgumentException("boo");
            });
            Assert.fail();
        } catch (RuntimeException e) {
            assertEquals("boo", e.getMessage());
        }
    }
    
    @Test
    public void testUncheckedCallThrowsError() {
        try {
            Sqs.uncheckedCall(() -> {
                throw new Error("boo");
            });
            Assert.fail();
        } catch (Error e) {
            assertEquals("boo", e.getMessage());
        }
    }
    
    @Test
    public void testUncheckedCallThrowsCheckedException() {
        try {
            Sqs.uncheckedCall(() -> {
                throw new Exception("boo");
            });
            Assert.fail();
        } catch (RuntimeException e) {
            assertEquals("boo", e.getCause().getMessage());
        }
    }
}
