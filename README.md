# rxjava-aws
<a href="https://travis-ci.org/davidmoten/rxjava-aws"><img src="https://travis-ci.org/davidmoten/rxjava-aws.svg"/></a><br/>
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.davidmoten/rxjava-aws/badge.svg?style=flat)](https://maven-badges.herokuapp.com/maven-central/com.github.davidmoten/rxjava-aws)<br/>
[![codecov](https://codecov.io/gh/davidmoten/rxjava-aws/branch/master/graph/badge.svg)](https://codecov.io/gh/davidmoten/rxjava-aws)

RxJava 1.x utilities for AWS (SQS, S3, ...)

Status: *released to Maven Central*

* Represent an SQS queue as an `Observable<SqsMessage>`
* Full backpressure support
* Supports low latency delivery (using long-polling which blocks a thread)
* Supports higher latency delivery via scheduled polling (reduced thread blocking)

##Getting started
Add the rxjava-aws dependency to your pom.xml:

```java
<dependency>
    <groupId>com.github.davidmoten</groupId>
    <artifactId>rxjava-aws</artifactId>
    <version>VERSION_HERE</version>
</dependency>
```

##Reading messages from an AWS SQS queue
The method below blocks a thread (using long polling). 


```java
Func0<AmazonSQSClient> sqs = () -> ...;

Sqs.queueName("my-queue")
    // specify factory for Amazon SQS Client
   .sqsFactory(sqs)
   // get messages as observable
   .messages()
   .// process the message
   .doOnNext(m -> System.out.println(m.message()))
   // delete the message (if processing succeeded)
   .doOnNext(m -> m.deleteMessage())
   // log any errors
   .doOnError(e -> log.warn(e.getMessage(), e))
   // run in the background
   .subscribeOn(Schedulers.io())
   // any errors then delay and resubscribe (on an io thread)
   .retryWhen(RetryWhen.delay(30, TimeUnit.SECONDS).build(), 
              Schedulers.io())
   // go!
   .subscribe(subscriber);
```

Use `.interval` for scheduled polling:


```java
Sqs.queueName("my-queue")
    // specify factory for Amazon SQS Client
   .sqsFactory(sqs)
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
   .sqsFactory(sqs)
   // every 60 seconds check for messages and wait for up to 5 seconds
   .waitTimes(
       Observable.interval(60, TimeUnit.SECONDS, Scheduler.io()).map(x -> 5),
       TimeUnit.SECONDS)
   // get messages as observable
   .messages()
   ...
```

##Reading messages from an AWS SQS queue via S3 storage
SQS queues are restricted to String messages (legal xml characters only) with a maximum size of 256K (binary messages would be Base64 encoded). If you want to pass larger messages then one pattern is to store the message content in a resource in an S3 bucket and put the resource name on to the queue. To receive the message you read the identifier from the queue and retrieve the resource bytes from the S3 bucket. Once you've dealt with the whole message you delete the S3 resource then remove the message from the queue.  

To read and delete messages from an AWS queue in this way (with full backpressure support):

```java
Func0<AmazonSQSClient> sqs = () -> ...;
Func0<AmazonS3Client> s3 = () -> ...; 

Sqs.queueName("my-queue")
    // specify factory for Amazon SQS Client
   .sqsFactory(sqs)
   // specify S3 bucket name
   .bucketName("my-bucket")
   // specify factory for Amazon S3 Client
   .s3Factory(s3)
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
   // any errors then delay and resubscribe
   .retryWhen(RetryWhen.delay(30, TimeUnit.SECONDS).build(),
              Schedulers.io())
   // go!
   .subscribe(subscriber);
```  
##Deleting messages from the queue
`deleteMessage()` will work quite happily even if the source has been terminated/unsubscribed. While the source has not been terminated you get slightly better performance because the source's sqs and s3 client objects can be used to perform the delete.

```java
// get just one message
SqsMessage message = 
   Sqs.queueName("my-queue")
      .sqsFactory(sqs)
      .bucketName("my-bucket")
      .s3Factory(s3)
      .messages()
      .subscribeOn(Schedulers.io())
      .first().toBlocking().single();
      
// this will still work fine        
message.deleteMessage();
```  
