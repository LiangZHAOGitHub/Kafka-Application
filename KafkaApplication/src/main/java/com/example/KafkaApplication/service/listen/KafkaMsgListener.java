package com.example.KafkaApplication.service.listen;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

/**
 * Kafka message consumer.
 * */
@Service
public class KafkaMsgListener {

    private static final Logger logger = LoggerFactory.getLogger(KafkaMsgListener.class);

    @KafkaListener(topics = "first_topic")
    public void consumeMsg(String receivedMsg) {
        logger.info("Received message is : " + receivedMsg);
    }

}
