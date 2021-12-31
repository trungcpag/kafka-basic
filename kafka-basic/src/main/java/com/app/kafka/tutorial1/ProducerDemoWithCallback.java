package com.app.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoWithCallback {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,  StringSerializer.class.getName());
        //create producer

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);


        for(int i = 0; i < 1000; i++) {

            // create a producer record
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "id_" + i,"Hello word" + Integer.toString(i));

            // send data

            producer.send(record, (RecordMetadata recordMetadata, Exception e) -> {
                        if (e == null) {
                            logger.info("Received new Metadata: \n" + "Topic: " + recordMetadata.topic()
                                    + ", Partitions: " + recordMetadata.partition() + ", Offset: " + recordMetadata.offset() +
                                    ", Timestamp: " + recordMetadata.timestamp()
                            );
                        } else {
                            logger.error("error", e);
                        }
                    }
            );
        }
        producer.flush();
        producer.close();

    }
}
