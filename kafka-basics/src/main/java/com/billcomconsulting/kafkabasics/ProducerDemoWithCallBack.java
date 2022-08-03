package com.billcomconsulting.kafkabasics;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class ProducerDemoWithCallBack {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallBack.class.getSimpleName());
    public static void main(String[] args) {


        //crete producer properties
        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //properties.setProperty(ProducerConfig.ACKS_CONFIG,"");

        //create the producer
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);

        for (int i=0;i<10;i++) {
            //create a producer record
            log.info("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! "+i);
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "hello world "+ i );
            //send the data-asynchronous operation(it doesn't wait to go to the next line of code )
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    //executes every time a record is successfully sent or an exception is thrown
                    if (e == null) {
                        //the record was successfully  sent
                        log.info("Received new metadata / \n" +
                                "Topic: " + metadata.topic() + "\n" +
                                "Partition: " + metadata.partition() + "\n" +
                                "Offset: " + metadata.offset() + "\n" +
                                "Timestamp: " + metadata.timestamp());
                    } else {
                        log.error("Error while producing", e);
                    }
                }
            });

            //without flush or close the program will shut down before even the producer can send the record to kafka
            //lock on this line of code until the data been sent

            //synchronous
            producer.flush();


        }
        //flush and close the producer
        producer.close();
    }}




