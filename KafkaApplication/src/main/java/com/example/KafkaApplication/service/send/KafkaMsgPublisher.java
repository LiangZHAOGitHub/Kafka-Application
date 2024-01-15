package com.example.KafkaApplication.service.send;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Kafka message publisher.
 * */
@Service
public class KafkaMsgPublisher {

    private static final Logger logger = LoggerFactory.getLogger(KafkaMsgPublisher.class);

    @Value("${kafka.topic}")
    private String topic;

    private volatile int counter = 0;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Scheduled(fixedRate = 1000)
    public void sendMessage() {
        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, "Testing message " + counter);
        future.whenComplete((result, err) -> {
            if(Objects.isNull(err)) {
                ++counter;
                logger.info("This message has been successfully sent : " + result.getRecordMetadata().toString());
            } else {
                logger.error("An error has been occurred during message sending !");
            }
        });
    }
}
