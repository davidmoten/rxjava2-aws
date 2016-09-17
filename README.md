# rxjava-aws
RxJava 1.x utilities for AWS (SQS, S3, ...)

Status: *pre-alpha*

##Reading S3 stored messages from an AWS SQS queue

One solution to passing arbitrary size messages through SQS is to submit an identifier (using `UUID.randomUUID().toString()`) to an SQS queue where the identifier is the name of a resource in an S3 bucket. This is a useful pattern to meet the AWS imposed constraints on SQS message size (max 256K) and favours scalable consumption of messages from the queue (if you can put up with a little bit of S3 latency).

To support reading (and deleting) messages from an AWS queue in this way using RxJava and this library:

```java
Func0<AmazonSQSClient> sqs = () -> ...;
Func0<AmazonS3Client> s3 = () -> ...; 

// retry delays (capped exponential backoff)
Observable<Long> delays = 
    Observable
        .just(1, 2, 4, 8, 16, 30}
        .compose(Transformers.repeatLast());
        

Sqs.messagesViaS3(s3, sqs, queueName, bucketName)
   // process the message
   .doOnNext(System.out::println)
   // delete the message (if processing succeeded)
   .doOnNext(m -> m.deleteMessage())
   .subscribeOn(Schedulers.io())
   // any errors then delay and resubscribe
   .retryWhen(
       RetryWhen.delay(delays, TimeUnit.SECONDS).build())
   .subscribe(subscriber);
```  
##Deleting messages from the queue
Note particularly the call to `m.deleteMessage()`. If the source of messages has been unsubscribed before this call (we might want to process the message asynchronously) then the call to `m.deleteMessage()` won't be able to use the same sqs and s3 client objects that the Observable source used. In this case it will use the sqs and s3 client factories passed to the `Sqs.messagesViaS3` method to create new sqs and s3 client objects to do the delete. The new created client objects are discarded (available for gc) after the call to `m.deleteMessage` has completed.  

The result is that `deleteMessage()` will work quite happily even if the source has been disconnected:

```java
// get just one message
SqsMessageViaS3 message = 
    Sqs.messagesViaS3(s3, sqs, queueName, bucketName)
       .subscribeOn(Schedulers.io())
        .first().toBlocking().single();
// this will still work fine        
message.deleteMessage();
```  

 

