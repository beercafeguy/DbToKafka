package com.beercafeguy.data.stream.kafka;

import com.beercafeguy.data.db.dao.QueryExecutor;
import com.beercafeguy.data.util.PropertyFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class DbKafkaProducerThread extends Thread {

    private final Logger logger = LoggerFactory.getLogger(DbKafkaProducerThread.class);

    private final KafkaProducer<Integer, String> kafkaProducer;
    private final String topicName;
    private final Boolean isAsync;
    private final String query;

    private static Properties getProps() {
        Properties properties = null;
        try {
            return PropertyFactory.getProperties("src/main/resources/kafka.properties");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return properties;
    }

    public DbKafkaProducerThread(String topicName, String query) {
        logger.info("Setting up producer");
        Properties kafkaServerProps = getProps();
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServerProps.getProperty("bootstrep.servers"));
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "DBToKafka");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProducer = new KafkaProducer<>(props);
        this.topicName = topicName;
        this.isAsync = Integer.parseInt(kafkaServerProps.getProperty("kafka.consumer.sync")) == 1;
        this.query = query;
        logger.info("Producer created.");
    }

    @Override
    public void run() {
        ResultSet resultSet = null;
        try {
            logger.info("Execution request against DB...");
            resultSet = QueryExecutor.getResult(query);
            while (resultSet.next()) {
                Integer key = resultSet.getInt("user_id");
                logger.info("Message Key: "+key);
                String keyStr = Integer.toString(key);
                StringBuffer buffer = new StringBuffer();

                buffer.append("user_name ->");
                buffer.append(resultSet.getString("user_name"));
                buffer.append("|");

                buffer.append("user_email ->");
                buffer.append(resultSet.getString("user_email"));
                buffer.append("|");

                buffer.append("created_at ->");
                buffer.append(resultSet.getString("created_at"));
                buffer.append("|");

                String value = buffer.toString();

                logger.info("Message Value: "+value);


                long startTime = System.currentTimeMillis();
                if (isAsync) {
                    logger.info("Producer type is async");
                    ProducerRecord<Integer, String> record = new ProducerRecord<Integer, String>(topicName, key, value);
                    try {
                        kafkaProducer.send(record).get();
                        logger.info("Sent message with key : (" + key + ")");
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    }
                } else {
                    logger.info("Producer type is sync");
                    ProducerRecord<Integer, String> record = new ProducerRecord<Integer, String>(topicName, key, value);
                    kafkaProducer.send(record, new DbKafkaProducerCallback(key, value, startTime));
                    logger.info("Sent message with key: (" + key + ")");

                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }
}
