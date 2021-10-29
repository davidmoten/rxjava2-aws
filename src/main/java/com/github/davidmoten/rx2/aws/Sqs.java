package com.github.davidmoten.rx2.aws;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.github.davidmoten.guavamini.Preconditions;
import com.github.davidmoten.rx2.aws.SqsMessage.Service;

import io.reactivex.Emitter;
import io.reactivex.Flowable;
import io.reactivex.Scheduler;
import io.reactivex.functions.BiConsumer;
import io.reactivex.schedulers.Schedulers;

public final class Sqs {

    private static final String HTTPS = "https://";

    private Sqs() {
        // prevent instantiation
    }

    public static SqsBuilder queueName(String queueName) {
        return new SqsBuilder(SqsQueue.fromQueueName(queueName));
    }

    public static BuilderWithOwnerAccountId ownerAccountId(String ownerAccountId) {
        return new BuilderWithOwnerAccountId(ownerAccountId);
    }

    public static final class BuilderWithOwnerAccountId {
        private final String ownerAccountId;

        BuilderWithOwnerAccountId(String ownerAccountId) {
            this.ownerAccountId = ownerAccountId;
        }

        SqsBuilder queueName(String queueName) {
            return new SqsBuilder(SqsQueue.fromQueueNameAndOwnerAccountId(queueName, ownerAccountId));
        }
    }

    public static SqsBuilder queueUrl(String queueUrl) {
        Preconditions.checkArgument(queueUrl.startsWith(HTTPS), "queueUrl must be an https url: " + queueUrl);
        return new SqsBuilder(SqsQueue.fromQueueUrl(queueUrl));
    }

    public static String sendToQueueUsingS3(AmazonSQS sqs, String queueUrl, AmazonS3 s3, String bucketName,
            Map<String, String> headers, byte[] message, Callable<String> s3IdFactory) {
        Preconditions.checkNotNull(sqs);
        Preconditions.checkNotNull(s3);
        Preconditions.checkNotNull(queueUrl);
        Preconditions.checkNotNull(bucketName);
        Preconditions.checkNotNull(message);
        String s3Id;
        try {
            s3Id = s3IdFactory.call();
        } catch (final Exception e1) {
            throw new RuntimeException(e1);
        }
        final ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(message.length);
        for (final Entry<String, String> header : headers.entrySet()) {
            metadata.setHeader(header.getKey(), header.getValue());
        }
        s3.putObject(bucketName, s3Id, new ByteArrayInputStream(message), metadata);
        try {
            sqs.sendMessage(queueUrl, s3Id);
        } catch (final RuntimeException e) {
            try {
                s3.deleteObject(bucketName, s3Id);
                throw e;
            } catch (final RuntimeException e2) {
                throw new io.reactivex.exceptions.CompositeException(e, e2);
            }
        }
        return s3Id;
    }

    public static String sendToQueueUsingS3(AmazonSQS sqs, String queueUrl, AmazonS3 s3, String bucketName,
            byte[] message, Callable<String> s3IdFactory) {
        return sendToQueueUsingS3(sqs, queueUrl, s3, bucketName, Collections.emptyMap(), message, s3IdFactory);
    }

    public static String sendToQueueUsingS3(AmazonSQS sqs, String queueUrl, AmazonS3 s3, String bucketName,
            byte[] message) {
        return sendToQueueUsingS3(sqs, queueUrl, s3, bucketName, message,
                () -> UUID.randomUUID().toString().replace("-", ""));
    }

    public static String sendToQueueUsingS3(AmazonSQS sqs, String queueUrl, AmazonS3 s3, String bucketName,
            Map<String, String> headers, byte[] message) {
        return sendToQueueUsingS3(sqs, queueUrl, s3, bucketName, headers, message,
                () -> UUID.randomUUID().toString().replace("-", ""));
    }

    public static final class SqsBuilder {
        private final SqsQueue queue;
        private Callable<AmazonSQS> sqs = null;
        private Optional<Callable<AmazonS3>> s3 = Optional.empty();
        private Optional<String> bucketName = Optional.empty();
        private Optional<Flowable<Integer>> waitTimesSeconds = Optional.empty();
        private Consumer<? super String> logger = (String s) -> {};
        private Runnable prePoll = () -> {};
        private Consumer<? super Optional<Throwable>> postPoll = (Optional<Throwable> t) -> {};

        SqsBuilder(SqsQueue queue) {
            Preconditions.checkNotNull(queue);
            this.queue = queue;
        }

        public ViaS3Builder bucketName(String bucketName) {
            this.bucketName = Optional.of(bucketName);
            return new ViaS3Builder(this);
        }

        public SqsBuilder sqsFactory(Callable<AmazonSQS> sqsFactory) {
            this.sqs = sqsFactory;
            return this;
        }

        public SqsBuilder waitTimes(Flowable<? extends Number> waitTimesSeconds, TimeUnit unit) {
            this.waitTimesSeconds = Optional
                    .of(waitTimesSeconds.map(x -> (int) unit.toSeconds(Math.round(x.doubleValue()))));
            return this;
        }

        public SqsBuilder interval(int interval, TimeUnit unit, Scheduler scheduler) {
            return waitTimes( //
                    Flowable.just(0) //
                            .concatWith(Flowable.interval(interval, unit, scheduler).map(x -> 0)),
                    TimeUnit.SECONDS);
        }

        public SqsBuilder interval(int interval, TimeUnit unit) {
            return interval(interval, unit, Schedulers.io());
        }

        public SqsBuilder logger(Consumer<? super String> logger) {
            this.logger = logger;
            return this;
        }
        
        public SqsBuilder prePoll(Runnable prePoll) {
        	this.prePoll = prePoll;
        	return this;
        }
        
        public SqsBuilder postPoll(Consumer<? super Optional<Throwable>> postPoll) {
        	this.postPoll = postPoll;
        	return this;
        }

        public Flowable<SqsMessage> messages() {
            return Sqs.messages(sqs, s3, queue, bucketName, waitTimesSeconds, logger, prePoll, postPoll);
        }

    }

    public static final class ViaS3Builder {

        private final SqsBuilder sqsBuilder;

        public ViaS3Builder(SqsBuilder sqsBuilder) {
            this.sqsBuilder = sqsBuilder;
        }

        public SqsBuilder s3Factory(Callable<AmazonS3> s3Factory) {
            sqsBuilder.s3 = Optional.of(s3Factory);
            return sqsBuilder;
        }

    }

    static Flowable<SqsMessage> messages(Callable<AmazonSQS> sqsFactory, Optional<Callable<AmazonS3>> s3Factory,
            SqsQueue queue, Optional<String> bucketName, Optional<Flowable<Integer>> waitTimesSeconds,
            Consumer<? super String> logger, Runnable prePoll, Consumer<? super Optional<Throwable>> postPoll) {
        Preconditions.checkNotNull(sqsFactory);
        Preconditions.checkNotNull(s3Factory);
        Preconditions.checkNotNull(queue);
        Preconditions.checkNotNull(bucketName);
        Preconditions.checkNotNull(waitTimesSeconds);
        Preconditions.checkNotNull(logger);
        Preconditions.checkNotNull(prePoll);
        Preconditions.checkNotNull(postPoll);
        return Flowable.using(sqsFactory,
                sqs -> createFlowableWithSqs(sqs, s3Factory, sqsFactory, queue, bucketName, waitTimesSeconds, logger, prePoll, postPoll),
                sqs -> sqs.shutdown());
    }

    private static Flowable<SqsMessage> createFlowableWithSqs(AmazonSQS sqs, Optional<Callable<AmazonS3>> s3Factory,
            Callable<AmazonSQS> sqsFactory, SqsQueue queue, Optional<String> bucketName,
            Optional<Flowable<Integer>> waitTimesSeconds, Consumer<? super String> logger,
            Runnable prePoll,	Consumer<? super Optional<Throwable>> postPoll) {

        return Flowable.using(() -> s3Factory.map(x -> {
            try {
                return x.call();
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        }), //
                s3 -> createFlowableWithS3(sqs, s3Factory, sqsFactory, queue, bucketName, s3, waitTimesSeconds, logger, prePoll, postPoll),
                s3 -> s3.ifPresent(Util::shutdown));
    }

    private static Flowable<SqsMessage> createFlowableWithS3(AmazonSQS sqs, Optional<Callable<AmazonS3>> s3Factory,
            Callable<AmazonSQS> sqsFactory, SqsQueue queue, Optional<String> bucketName, Optional<AmazonS3> s3,
            Optional<Flowable<Integer>> waitTimesSeconds, Consumer<? super String> logger, Runnable prePoll,
			Consumer<? super Optional<Throwable>> postPoll) {
        final Service service = new Service(s3Factory, sqsFactory, s3, sqs, queue.getQueueUrl(sqs), bucketName);
        if (waitTimesSeconds.isPresent()) {
            return createFlowablePolling(waitTimesSeconds.get(), service, logger, prePoll, postPoll);
        } else {
            return createFlowableContinousLongPolling(service, logger, prePoll, postPoll);
        }
    }

    private static Flowable<SqsMessage> createFlowablePolling(Flowable<Integer> waitTimesSeconds, Service service,
            Consumer<? super String> logger, Runnable prePoll, Consumer<? super Optional<Throwable>> postPoll) {
        return waitTimesSeconds.flatMap(n -> get(n, service, logger, prePoll, postPoll), 1);
    }

    private static Flowable<SqsMessage> get(int waitTimeSeconds, Service service, Consumer<? super String> logger, 
    										Runnable prePoll, Consumer<? super Optional<Throwable>> postPoll) {
		Flowable<SqsMessage> flowable = Flowable.just(service.sqs.receiveMessage(request(service.queueUrl, waitTimeSeconds)) //
                .getMessages() //
                .stream() //
                .map(m -> Sqs.getNextMessage(m, service)) //
            .collect(Collectors.toList())) //
            .concatWith(Flowable.defer(() -> {
            	try {
            		prePoll.run();
            		ReceiveMessageResult messagesResult = service.sqs.receiveMessage(request(service.queueUrl, 0));
            		postPoll.accept(Optional.empty());
            		
                	return Flowable.just(messagesResult.getMessages() //
			                       .stream() //
			                       .map(m -> Sqs.getNextMessage(m, service)) //
			                       .collect(Collectors.toList()));
            	} catch(Throwable t) {
            		postPoll.accept(Optional.of(t));
            		throw t;
            	}
            }) //
            .repeat()) //
            .takeWhile(list -> !list.isEmpty()) //
            .flatMapIterable(x -> x) //
            .filter(opt -> opt.isPresent()) //
            .map(opt -> opt.get());
		
		return flowable;
	}

    private static Flowable<SqsMessage> createFlowableContinousLongPolling(Service service,
            Consumer<? super String> logger,
            Runnable prePoll,
			Consumer<? super Optional<Throwable>> postPoll) {
        final ContinuousLongPollingSyncOnSubscribe c = new ContinuousLongPollingSyncOnSubscribe(service, logger, prePoll, postPoll);
        return Flowable.generate(c, c);
    }

    private static final class ContinuousLongPollingSyncOnSubscribe
            implements Callable<State>, BiConsumer<State, Emitter<SqsMessage>> {

        private final Service service;

        private ReceiveMessageRequest request;
        private Consumer<? super String> logger;
        private Runnable prePoll;
        private Consumer<? super Optional<Throwable>> postPoll;

        public ContinuousLongPollingSyncOnSubscribe(Service service, Consumer<? super String> logger, Runnable prePoll,
        											Consumer<? super Optional<Throwable>> postPoll) {
            this.service = service;
            this.logger = logger;
            this.prePoll = prePoll;
            this.postPoll = postPoll;
        }

        @Override
        public State call() {
            request = new ReceiveMessageRequest(service.queueUrl) //
                    .withWaitTimeSeconds(20) //
                    .withMaxNumberOfMessages(10);
            return new State(new LinkedList<>());
        }

        @Override
        public void accept(State state, Emitter<SqsMessage> emitter) throws Exception {
            final Queue<Message> q = state.queue;
            Optional<SqsMessage> next = Optional.empty();
            while (!next.isPresent()) {
                while (q.isEmpty()) {
                    logger.accept("long polling for messages on queue=" + service.queueUrl);
                    try {
                        prePoll.run();
                    	final ReceiveMessageResult result = service.sqs.receiveMessage(request);
                    	postPoll.accept(Optional.empty());
                    	q.addAll(result.getMessages());
                    } catch(Throwable t) {
                    	postPoll.accept(Optional.of(t));
                    	throw t;
                    }
                }
                final Message message = q.poll();
                next = getNextMessage(message, service);
            }
            emitter.onNext(next.get());
        }

    }

    static Optional<SqsMessage> getNextMessage(Message message, Service service) {
        if (service.bucketName.isPresent()) {
            final String s3Id = message.getBody();
            if (!service.s3.get().doesObjectExist(service.bucketName.get(), s3Id)) {
                service.sqs.deleteMessage(service.queueUrl, message.getReceiptHandle());
                return Optional.empty();
            } else {
                final S3Object object = service.s3.get().getObject(service.bucketName.get(), s3Id);
                final byte[] content = readAndClose(object.getObjectContent());
                final long timestamp = object.getObjectMetadata().getLastModified().getTime();
                final SqsMessage mb = new SqsMessage(message.getReceiptHandle(), content, timestamp, Optional.of(s3Id),
                        service);
                return Optional.of(mb);
            }
        } else {
            final SqsMessage mb = new SqsMessage(message.getReceiptHandle(),
                    message.getBody().getBytes(StandardCharsets.UTF_8), System.currentTimeMillis(), Optional.empty(),
                    service);
            return Optional.of(mb);
        }
    }

    private static final class State {

        final Queue<Message> queue;

        public State(Queue<Message> queue) {
            this.queue = queue;
        }

    }

    private static ReceiveMessageRequest request(String queueUrl, int waitTimeSeconds) {
        return new ReceiveMessageRequest(queueUrl).withMaxNumberOfMessages(20).withWaitTimeSeconds(waitTimeSeconds);
    }

    // Visible for testing
    static byte[] readAndClose(InputStream is) {
        Preconditions.checkNotNull(is);
        try {
            final ByteArrayOutputStream bos = new ByteArrayOutputStream();
            final byte[] bytes = new byte[8192];
            int n;
            while ((n = is.read(bytes)) != -1) {
                bos.write(bytes, 0, n);
            }
            return bos.toByteArray();
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

}
