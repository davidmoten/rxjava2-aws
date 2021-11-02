# rxjava2-aws
<a href="https://github.com/davidmoten/rxjava2-aws/actions/workflows/ci.yml"><img src="https://github.com/davidmoten/rxjava2-aws/actions/workflows/ci.yml/badge.svg"/></a><br/>
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.davidmoten/rxjava2-aws/badge.svg?style=flat)](https://maven-badges.herokuapp.com/maven-central/com.github.davidmoten/rxjava2-aws)<br/>
[![codecov](https://codecov.io/gh/davidmoten/rxjava2-aws/branch/master/graph/badge.svg)](https://codecov.io/gh/davidmoten/rxjava2-aws)

RxJava 2.x utilities for AWS (SQS, S3, ...)

RxJava 1.x support is at [rxjava-aws](https://github.com/davidmoten/rxjava-aws).

Status: *released to Maven Central*

* Represent an SQS queue as a `Flowable<SqsMessage>`
* Full backpressure support
* Supports low latency delivery (using long-polling which blocks a thread)
* Supports higher latency delivery via scheduled polling (reduced thread blocking)
* 100% unit test coverage

Maven [reports](https://davidmoten.github.io/rxjava2-aws/index.html) including [javadocs](https://davidmoten.github.io/rxjava2-aws/apidocs/index.html)

## Getting started
Add the rxjava2-aws dependency to your pom.xml:

```java
<dependency>
    <groupId>com.github.davidmoten</groupId>
    <artifactId>rxjava2-aws</artifactId>
    <version>VERSION_HERE</version>
</dependency>
```

## Reading messages from an AWS SQS queue
The method below blocks a thread (using long polling). When demand exists it connects to the AWS REST API (using the Amazon Java SDK) and blocks up to 20s waiting for a message. IO-wise it's cheap but of course comes with the expense of blocking a thread. Note that as backpressure is supported while no requests for messages exist the REST API will not be called.

```java
Sqs.queueName("my-queue")
    // specify factory for Amazon SQS Client
   .sqsFactory(() -> AmazonSQSClientBuilder.standard().withRegion(region).build())
   // log polls
   .logger(System.out::println)
   // Action that occurs before SQS poll
   .prePoll(() -> {/* Do something */})
   // Action that occurs post SQS poll providing a Throwable if an Throwable event occurs
   .postPoll((Optional<Throwable> t) -> {/* do something */})
   // perform action using the poll date
   .lastPollDate(pollDate -> System.out::println)
   // get messages as observable
   .messages()
   // process the message
   .doOnNext(m -> System.out.println(m.message()))
   // delete the message (if processing succeeded)
   .doOnNext(m -> m.deleteMessage())
   // log any errors
   .doOnError(e -> log.warn(e.getMessage(), e))
   // run in the background
   .subscribeOn(Schedulers.io())
   // any errors then delay and resubscribe (on an io thread)
   .retryWhen(RetryWhen
         .delay(30, TimeUnit.SECONDS) 
         .scheduler(Schedulers.io())
         .build())
   // go!
   .subscribe(subscriber);
```

**Note:** In case of a (transient) error (eg. AWS rate limit exceeded errors), it's useful to have the `RetryWhen` clause in place. Note that in case of an error, the old sqs client gets disposed, and a new one gets created using the `sqsFactory` provided.

**Note:** To use the `RetryWhen` builder requires [*rxjava2-extras*](https://github.com/davidmoten/rxjava2-extras) dependency.

```java
<dependency>
    <groupId>com.github.davidmoten</groupId>
    <artifactId>rxjava2-extras</artifactId>
    <version>VERSION_HERE</version>
</dependency>
```
### Options for specifying the queue
Here are some variants:

```java
Sqs.queueName(queueName)...
Sqs.ownerAccountId(accountId).queueName(queueName)...
Sqs.queueUrl(queueUrl)...
```
### Scheduled polling
Use `.interval` for scheduled polling:


```java
Sqs.queueName("my-queue")
    // specify factory for Amazon SQS Client
   .sqsFactory(() -> ...)
   // every 60 seconds check for messages
   .interval(60, TimeUnit.SECONDS, Schedulers.io())
   // get messages as observable
   .messages()
   ...
```

or for lower level control of scheduled polling use `.waitTimes`:

```java
Sqs.queueName("my-queue")
    // specify factory for Amazon SQS Client
   .sqsFactory(() -> ...)
   // every 60 seconds check for messages and wait for up to 5 seconds
   .waitTimes(
       Observable.interval(60, TimeUnit.SECONDS, Scheduler.io()).map(x -> 5),
       TimeUnit.SECONDS)
   // get messages as observable
   .messages()
   ...
```

## Reading messages from an AWS SQS queue via S3 storage
SQS queues are restricted to String messages (legal xml characters only) with a maximum size of 256K (binary messages would be Base64 encoded). If you want to pass larger messages then one pattern is to store the message content in a resource in an S3 bucket and put the resource name on to the queue. To receive the message you read the identifier from the queue and retrieve the resource bytes from the S3 bucket. Once you've dealt with the whole message you delete the S3 resource then remove the message from the queue.  

To read and delete messages from an AWS queue in this way (with full backpressure support):

```java
Sqs.queueName("my-queue")
    // specify factory for Amazon SQS Client
   .sqsFactory(() -> AmazonSQSClientBuilder.standard().withRegion(region).build())
   // specify S3 bucket name
   .bucketName("my-bucket")
   // specify factory for Amazon S3 Client
   .s3Factory(() -> AmazonS3ClientBuilder.standard().withRegion(region).build())
   // get messages as observable
   .messages()
   // process the message
   .doOnNext(System.out::println)
   // delete the message (if processing succeeded)
   .doOnNext(m -> m.deleteMessage())
   // log any errors
   .doOnError(e -> log.warn(e.getMessage(), e))
   // run in the background
   .subscribeOn(Schedulers.io())
   // any errors then delay and resubscribe (on an io thread)
   .retryWhen(RetryWhen
         .delay(30, TimeUnit.SECONDS) 
         .scheduler(Schedulers.io())
         .build())
   // go!
   .subscribe(subscriber);
```  
Note that to use the `RetryWhen` builder requires [*rxjava2-extras*](https://github.com/davidmoten/rxjava2-extras) dependency.

## Sending messages to a an AWS SQS queue via S3 storage

To place a message on an AWS SQS queue for picking up via the routine above:

```java
String s3Id = 
  Sqs.sendToQueueUsingS3(sqs, queueUrl, s3, bucketName, messageBytes, s3IdFactory);
```

## Deleting messages from the queue
`deleteMessage()` will work quite happily even if the source has been terminated/unsubscribed. While the source has not been terminated you get slightly better performance because the source's sqs and s3 client objects can be used to perform the delete.

```java
// get just one message
SqsMessage message = 
   Sqs.queueName("my-queue")
      .sqsFactory(() -> ...)
      .bucketName("my-bucket")
      .s3Factory(() -> ...)
      .messages()
      .subscribeOn(Schedulers.io())
      .blockingFirst();
      
// this will still work fine        
message.deleteMessage();
```  
