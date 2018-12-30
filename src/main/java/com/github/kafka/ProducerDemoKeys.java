package com.github.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;


public class ProducerDemoKeys {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);


        for (int i = 0; i <= 10; i++) {
            String topic = "first_topic";
            String value ="Hello From Java " + i;
            String key =" Key " + i;

            logger.info("Key : " + key);
            //create a producer record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic, key,value);

            //send data

            producer.send(producerRecord, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // execute everytime when message send or exception thrown
                    if (e == null) {
                        logger.info("Received metadata \n" +
                                "Topic : " + recordMetadata.topic() + "\n" +
                                "Partition : " + recordMetadata.partition() + "\n" +
                                "Offset : " + recordMetadata.offset() + "\n" +
                                "TimeStamp :" + recordMetadata.timestamp()
                        );
                    } else {
                        logger.error("Error in Producer " + e);
                    }


                }
            }).get(); // this is async process
             producer.flush(); //flush data

        }
        producer.close(); // flush data and close
    }
}
