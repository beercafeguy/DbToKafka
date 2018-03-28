package com.beercafeguy.data.stream.kafka;

import com.beercafeguy.data.db.dao.QueryExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;

public class DbKafkaProducer {
    private static final Logger logger = LoggerFactory.getLogger(DbKafkaProducerThread.class);
    public static void main(String[] args) {

        String topicName="user_master_demo";
        logger.info("DB to kafka producer starting....");
        String query="select user_id,user_name,user_email,created_at from user_master";
        logger.info("Query to be executed:"+query);
        DbKafkaProducerThread producerThread = new DbKafkaProducerThread(topicName, query);
        producerThread.start();
    }
}
