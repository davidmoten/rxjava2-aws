package com.github.davidmoten.rx2.aws;

import java.util.Optional;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.github.davidmoten.guavamini.Preconditions;

final class SqsQueue {

    final Optional<String> queueUrl;

    final Optional<String> queueName;

    final Optional<String> ownerAccountId;

    static SqsQueue fromQueueName(String queueName) {
        return new SqsQueue(Optional.of(queueName), Optional.empty(), Optional.empty());
    }

    static SqsQueue fromQueueNameAndOwnerAccountId(String queueName, String ownerAccountId) {
        return new SqsQueue(Optional.of(queueName), Optional.of(ownerAccountId), Optional.empty());
    }

    static SqsQueue fromQueueUrl(String queueUrl) {
        return new SqsQueue(Optional.empty(), Optional.empty(), Optional.of(queueUrl));
    }

    private SqsQueue(Optional<String> queueName, Optional<String> ownerAccountId, Optional<String> queueUrl) {
        Preconditions.checkNotNull(queueName);
        Preconditions.checkNotNull(queueUrl);
        Preconditions.checkNotNull(ownerAccountId);
        Preconditions.checkArgument(!queueUrl.isPresent() || !queueName.isPresent() && !ownerAccountId.isPresent());
        Preconditions.checkArgument(queueUrl.isPresent() || queueName.isPresent());
        this.queueName = queueName;
        this.queueUrl = queueUrl;
        this.ownerAccountId = ownerAccountId;
    }

    public String getQueueUrl(AmazonSQS sqs) {
        return queueUrl.orElseGet(() -> {
            GetQueueUrlRequest req = new GetQueueUrlRequest(queueName.get());
            if (ownerAccountId.isPresent()) {
                req = req.withQueueOwnerAWSAccountId(ownerAccountId.get());
            }
            return sqs.getQueueUrl(req).getQueueUrl();
        });
    }

}
