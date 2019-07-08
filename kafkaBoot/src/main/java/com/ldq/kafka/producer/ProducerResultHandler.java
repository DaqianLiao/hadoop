package com.ldq.kafka.producer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Slf4j
@Component
public class ProducerResultHandler implements ListenableFutureCallback<SendResult<String, String>> {
    @Override
    public void onFailure(Throwable throwable) {
        log.error("send msg error! caused by {}", throwable.getCause());
    }

    @Override
    public void onSuccess(SendResult<String, String> result) {
        log.info("send success! metadata = {}", result.getRecordMetadata());
    }
}
