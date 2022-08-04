package com.billcomconsulting.kafkabasics;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.omg.SendingContext.RunTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoWithShutdown {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());



    //adding the shutdown hook


    public static void main(String[] args) {
        //get a reference to the current Thread
        final Thread mainThread = Thread.currentThread();

        log.info("I am a kafka Consumer");
        String groupId ="my-third-app";
        String topic ="demo_java";

        //set up kafka propriety
        Properties properties=new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        //create consumer
        KafkaConsumer<String,String> consumer= new KafkaConsumer<>(properties);

        //adding the shutdown Hook
        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run() {
                super.run();
            }
        });


        //subscribe consumer to our topic(s) .asList
        consumer.subscribe(Collections.singletonList(topic));

        //poll for new data
        while (true){
            log.info("polling");
            ConsumerRecords<String,String> records=
                    consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String,String> record :records)
            {
                log.info("Key: "+record.key()+", Value: "+record.value());
                log.info("Partition: "+record.partition() +", Offset: "+record.offset());
            }
        }
    }
}