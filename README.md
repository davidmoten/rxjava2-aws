# rxjava-aws
RxJava 1.x utilities for AWS (SQS, S3, ...)

Status: *pre-alpha*

##Reading messages from an AWS SQS queue

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

##Reading messages from an AWS SQS queue via S3 storage

One solution to passing arbitrary size messages through SQS is to submit an identifier (using `UUID.randomUUID().toString()`) to an SQS queue where the identifier is the name of a resource in an S3 bucket. This is a useful pattern to meet the AWS imposed constraints on SQS message size (max 256K) and favours scalable consumption of messages from the queue (if you can put up with a little bit of S3 latency).

To support reading (and deleting) messages from an AWS queue in this way using RxJava and this library:

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
SqsMessageViaS3 message = 
   Sqs.queueName("my-queue")
      .sqsFactory(sqs)
      .messages()
      .s3Factory(s3)
      .bucketName("my-bucket")
      .messages()
      .subscribeOn(Schedulers.io())
      .first().toBlocking().single();
      
// this will still work fine        
message.deleteMessage();
```  

 

