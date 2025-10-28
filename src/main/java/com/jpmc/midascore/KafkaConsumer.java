package com.jpmc.midascore;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
public class KafkaConsumer {

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final UserRepository userRepository;
    private final TransactionRecordRepository transactionRecordRepository;
    private final RestTemplate restTemplate;

    @Autowired
    public KafkaConsumer(UserRepository userRepository,
                         TransactionRecordRepository transactionRecordRepository,
                         RestTemplate restTemplate) {
        this.userRepository = userRepository;
        this.transactionRecordRepository = transactionRecordRepository;
        this.restTemplate = restTemplate;
    }

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core-group")
    public void listen(String message) {
        try {
            Transaction transaction = objectMapper.readValue(message, Transaction.class);
            System.out.println("Received transaction: " + transaction);

            UserRecord sender = userRepository.findById(transaction.getSenderId());
            UserRecord recipient = userRepository.findById(transaction.getRecipientId());

            if (sender != null && recipient != null && sender.getBalance() >= transaction.getAmount()) {

                // ✅ Step 1: Call the Incentive API
                Incentive incentive = restTemplate.postForObject(
                        "http://localhost:8080/incentive",
                        transaction,
                        Incentive.class
                );

                float incentiveAmount = (incentive != null) ? incentive.getAmount() : 0.0f;

                // ✅ Step 2: Update balances
                sender.setBalance(sender.getBalance() - transaction.getAmount());
                recipient.setBalance(recipient.getBalance() + transaction.getAmount() + incentiveAmount);

                userRepository.save(sender);
                userRepository.save(recipient);

                // ✅ Step 3: Save transaction including incentive
                TransactionRecord record = new TransactionRecord(sender, recipient, transaction.getAmount());
                record.setIncentive(incentiveAmount);
                transactionRecordRepository.save(record);

                System.out.println("✅ Transaction processed with incentive " + incentiveAmount);
            } else {
                System.out.println("❌ Invalid transaction. Skipped.");
            }

            // Optional: Print wilbur's balance to debug
            for (UserRecord user : userRepository.findAll()) {
                if ("wilbur".equalsIgnoreCase(user.getName())) {
                    System.out.println("🏦 Current balance of Wilbur: " + user.getBalance());
                    break;
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // ✅ Inner class to represent Incentive response
    static class Incentive {
        private float amount;

        public float getAmount() {
            return amount;
        }

        public void setAmount(float amount) {
            this.amount = amount;
        }
    }
}
