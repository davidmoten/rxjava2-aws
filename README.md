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
SQS queues are restricted to String messages (ASCII) with a maximum size of 256K (binary messages would be Base64 encoded). If you want to pass larger messages then one pattern is to store the message content in a resource in an S3 bucket and put the resource name on to the queue. To receive the message you read the identifier from the queue and retrieve the resource bytes from the S3 bucket. Once you've dealt with the whole message you delete the S3 resource then remove the message from the queue.  

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

 

