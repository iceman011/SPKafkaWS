package com.spboot.service;

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

import com.spboot.websocket.WSController;

@Component
public class KafkaRec {

	  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaRec.class);

	  private CountDownLatch latch = new CountDownLatch(1);

	  @Autowired
	  WSController wsController;
	  

	  public CountDownLatch getLatch() {
	    return latch;
	  }

	  @KafkaListener(topics = "${kafka.topic.boot}")
	  public void receive(ConsumerRecord<?, ?> consumerRecord) {
	    LOGGER.info("received payload='{}'", consumerRecord.toString());
	    latch.countDown();
	    
	    //wsCntStrm.createTrackingStreamsInstance();
	    
	    wsController.sendWSMessage(consumerRecord.value().toString());
	   
	  }
	}
