package com.jpmc.midascore.service;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.UserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class TransactionService {
    private static final Logger logger = LoggerFactory.getLogger(TransactionService.class);
    
    private final UserRepository userRepository;

    public TransactionService(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    @KafkaListener(topics = "${general.kafka-topic}")
    @Transactional
    public void processTransaction(Transaction transaction) {
        logger.debug("Processing transaction: {}", transaction);
        
        try {
            UserRecord sender = userRepository.findById(transaction.getSenderId());
            UserRecord recipient = userRepository.findById(transaction.getRecipientId());
            
            if (sender == null) {
                logger.error("Sender with ID {} not found", transaction.getSenderId());
                return;
            }
            
            if (recipient == null) {
                logger.error("Recipient with ID {} not found", transaction.getRecipientId());
                return;
            }
            
            if (sender.getBalance() < transaction.getAmount()) {
                logger.error("Insufficient funds for sender ID {}: balance={}, amount={}", 
                    transaction.getSenderId(), sender.getBalance(), transaction.getAmount());
                return;
            }
            
            // Process the transaction
            sender.setBalance(sender.getBalance() - transaction.getAmount());
            recipient.setBalance(recipient.getBalance() + transaction.getAmount());
            
            userRepository.save(sender);
            userRepository.save(recipient);
            
            logger.info("Transaction processed successfully: {} -> {} amount: {}", 
                sender.getName(), recipient.getName(), transaction.getAmount());
                
        } catch (Exception e) {
            logger.error("Error processing transaction: {}", transaction, e);
        }
    }
}