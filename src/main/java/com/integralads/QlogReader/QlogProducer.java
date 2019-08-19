package com.integralads.QlogReader;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class QlogProducer {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(QlogProducer.class);
        QlogParser qlogParser = new QlogParser();

        String bootstrapServer = "127.0.0.1:9092";
        String topic = "POC-qlogReader";

        //Setup Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        // Creating the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // Create a record
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, qlogParser.parseOutput());

        //sending data
        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                // executes everytime a record is successfully sent or an exception is thrown
                if (e == null) {
                    // is successfully sent
                    logger.info("Received new metadata. \n" +
                                    "Topic:" + recordMetadata.topic() + "\n",
                            "Partition:" + recordMetadata.partition() + "\n",
                            "Offset:" + recordMetadata.offset() + "\n",
                            "TimeStamp" + recordMetadata.timestamp());
                } else {
                    logger.error("Error", e);
                }
            }
        });

        // flush data
        producer.flush();

        // flush and close producer
        producer.close();
    }
}

