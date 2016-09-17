# rxjava-aws
RxJava 1.x utilities for AWS (SQS, S3, ...)

Status: *pre-alpha*

##Reading messages from an AWS SQS queue

One solution to passing arbitrary size messages through SQS is to submit an identifier ((using `UUID.randomUUID().toString()`) to an SQS queue where the identifier is the name of a resource in an S3 bucket. This is a useful pattern to meet the AWS imposed constraints on SQS message size (max 256K) and favours scalable consumption of messages from the queue (if you can put up with a little bit of S3 latency).

To support reading messages from an AWS queue in this way using RxJava and this library:

```java
Func0<AmazonSQSClient> sqs = () -> ...;
Func0<AmazonS3Client> s3 = () -> ...; 

//retry delays (capped exponential backoff)
Observable<Long> delays = 
    Observable
        .just(1, 2, 4, 8, 16, 30}
        .compose(Transformers.repeatLast());
        
Observable<SqsMessgeViaS3> messages = 
    Sqs.messagesViaS3(s3, sqs, queueName, bucketName)
       .doOnNext(System.out::println)
	   .subscribeOn(Schedulers.io())
	   .retryWhen(RetryWhen.delay(delays, TimeUnit.SECONDS).build())
	   .subscribe(subscriber);
```  

