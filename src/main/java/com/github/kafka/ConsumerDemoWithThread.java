package com.github.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.metrics.stats.Count;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

        CountDownLatch latch = new CountDownLatch(1);

        Runnable myConsumer = new ConsumerThread(latch);
        Thread thread = new Thread(myConsumer);
        thread.start();

        //add a shutdown hook

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {

            logger.info("Shutdown hook");
            ((ConsumerThread) myConsumer).shutdown();
            try {
                latch.wait();
            } catch (InterruptedException e) {
                logger.error("Application got error 1111");
            }finally {
                logger.info("Application is closing 2222");
            }
        }
        ));


        try {
            latch.wait();
        } catch (InterruptedException e) {
           logger.error("Application got error");
        }finally {
           logger.info("Application is closing");
        }


    }
}


 class ConsumerThread implements  Runnable{

     CountDownLatch latch;
     KafkaConsumer<String,String> kafkaConsumer;
     Logger logger = LoggerFactory.getLogger(ConsumerThread.class);
     String topic = "first_topic";

    public ConsumerThread(CountDownLatch latch){
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"my-consumer-group8");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        this.latch=latch;
        kafkaConsumer = new KafkaConsumer<String, String>(properties);
        kafkaConsumer.subscribe(Collections.singleton(topic));

    }

     @Override
     public void run() {
        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMinutes(100));

                for (ConsumerRecord<String, String> record : consumerRecords) {
                    logger.info("Key  " + record.key());
                    logger.info("Value  " + record.value());
                    logger.info("Parition  " + record.partition());
                    logger.info("Offset  " + record.offset());
                }

            }
        }catch(WakeupException w){
            logger.info("Exception caugh WakeupException");
         }finally {
            System.out.println("Closing Consumer");
            kafkaConsumer.close();
            latch.countDown();
        }
     }

     public void shutdown(){
        kafkaConsumer.wakeup();
     }
 }
