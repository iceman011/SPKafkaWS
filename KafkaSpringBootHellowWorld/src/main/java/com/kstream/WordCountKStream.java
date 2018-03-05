package com.kstream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;

import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Controller;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;

import com.spboot.websocket.WSController;

/**
 * @author jkorab
 */
@Component
public class WordCountKStream {

private static final Logger LOGGER = LoggerFactory.getLogger(WordCountKStream.class);

	@Autowired
	 WSController wsController;
	  
    private String topic = "hello_topic";
    private KafkaStreams streams;

    //@PostConstruct
    public void runStream() {
        Serde<String> stringSerde = Serdes.String();

        Properties config = new Properties();
        final String version = "0.2";
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-example-" + version);
        config.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, stringSerde.getClass().getName());
        config.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, stringSerde.getClass().getName());
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");

        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Pattern pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);
        KStreamBuilder wordLines = new KStreamBuilder();
		
		KStream<String,String> words = wordLines.stream(stringSerde, stringSerde, topic);
		//https://stackoverflow.com/questions/39327868/print-kafka-stream-input-out-to-console
		
        
           words.flatMapValues(value -> Arrays.asList(pattern.split(value.toLowerCase())))
                .filter((key, value) -> value.trim().length() > 0)
                .map((key, value) -> new KeyValue<>(value, value))
                .countByKey("vehicle_id")
                .toStream()
                .to(stringSerde, Serdes.Long(), "words-with-counts-" + version);
				
			words.foreach(new ForeachAction<String, String>() {
			public void apply(String key, String value) {
				LOGGER.info(" looping " +key + ": " + value);
				wsController.sendWSMessage(value.toString());
			}
		 });
		 
        streams = new KafkaStreams(wordLines, config);
		
		
		LOGGER.info("KStream Started");
        streams.start();
    }

    @PreDestroy
    public void closeStream() {
        streams.close();
    }
}