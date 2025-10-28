package com.jpmc.midascore;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jpmc.midascore.foundation.Transaction;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaProducer {
    private final String topic;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public KafkaProducer(@Value("${general.kafka-topic}") String topic, KafkaTemplate<String, String> kafkaTemplate) {
        this.topic = topic;
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(String transactionLine) {
        try {
            String[] transactionData = transactionLine.split(",\\s*");
            Transaction transaction = new Transaction(
                    Long.parseLong(transactionData[0]),
                    Long.parseLong(transactionData[1]),
                    Float.parseFloat(transactionData[2])
            );

            // convert the Transaction object to JSON string
            String jsonTransaction = objectMapper.writeValueAsString(transaction);
            kafkaTemplate.send(topic, jsonTransaction);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
