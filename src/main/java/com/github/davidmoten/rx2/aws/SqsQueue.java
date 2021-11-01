package com.github.davidmoten.rx2.aws;

import java.util.Optional;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.github.davidmoten.guavamini.Preconditions;
import com.github.davidmoten.guavamini.annotations.VisibleForTesting;

final class SqsQueue {

    private final Optional<String> queueUrl;

    private final Optional<GetQueueUrlRequest> request;

    static SqsQueue fromQueueName(String queueName) {
        return new SqsQueue(Optional.of(queueName), Optional.empty(), Optional.empty());
    }

    static SqsQueue fromQueueNameAndOwnerAccountId(String queueName, String ownerAccountId) {
        return new SqsQueue(Optional.of(queueName), Optional.of(ownerAccountId), Optional.empty());
    }

    static SqsQueue fromQueueUrl(String queueUrl) {
        return new SqsQueue(Optional.empty(), Optional.empty(), Optional.of(queueUrl));
    }

    @VisibleForTesting
    SqsQueue(Optional<String> queueName, Optional<String> ownerAccountId,
            Optional<String> queueUrl) {
        Preconditions.checkNotNull(queueName);
        Preconditions.checkNotNull(queueUrl);
        Preconditions.checkNotNull(ownerAccountId);
        Preconditions.checkArgument(queueUrl.isPresent() || queueName.isPresent());
        Preconditions.checkArgument(!queueUrl.isPresent() || !queueName.isPresent());
        Preconditions.checkArgument(queueName.isPresent() || !ownerAccountId.isPresent());
        this.queueUrl = queueUrl;
        this.request = queueUrl.isPresent() ? Optional.empty()
                : Optional.of(createRequest(queueName.get(), ownerAccountId));
    }

    private static GetQueueUrlRequest createRequest(String queueName,
            Optional<String> ownerAccountId) {
        Preconditions.checkNotNull(queueName);
        GetQueueUrlRequest r = new GetQueueUrlRequest(queueName);
        if (ownerAccountId.isPresent()) {
            r = r.withQueueOwnerAWSAccountId(ownerAccountId.get());
        }
        return r;
    }

    public String getQueueUrl(AmazonSQS sqs) {
        return queueUrl.orElseGet(() -> sqs.getQueueUrl(request.get()).getQueueUrl());
    }
}
