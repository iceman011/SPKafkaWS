package com.spboot.controller;

import java.util.concurrent.TimeUnit;

import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.spboot.service.KafkaRec;
import com.spboot.service.KafkaSender;
import com.kstream.analytics.tracking.TrackingAnalytic;


@RestController
@ComponentScan({ "com.spboot" })
@ComponentScan({ "com.kstream" })
//@RequestMapping(value = "/hellokafka/")
public class KafkaWebController {

	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaWebController.class);
	
		@Autowired
		KafkaSender kafkaSender;

		@Autowired
		KafkaRec kafkaRec;
		
		@Autowired
		TrackingAnalytic trackingKS;
		
		public static final String MAIN_TOPIC = "hello_topic";
		public static final String GROUP_BY_TOPIC = "group_by_topic";
		public static final String VEHICLE_BY_TOPIC = "vehicle_by_topic";
		public static final String EVENT_BY_TOPIC = "vehicle_by_event";
		
		@GetMapping(value = "/home")
		public String home() {
			//kafkaSender.send(message);

			return "home is called";
		}
		
		
		
		@GetMapping(value = "/hellokafka/producer")
		public String producer(@RequestParam("message") String message) {
			
			kafkaSender.send(MAIN_TOPIC,message);
			return "Message sent to the Kafka Topic java_in_use_topic Successfully";
			
		}

		@GetMapping(value = "/hellokafka/rec")
		public String rec() {
			try {
			kafkaRec.getLatch().await(10000, TimeUnit.MILLISECONDS);
			LOGGER.info(kafkaRec.getLatch().getCount() + "");
			}catch (Exception e) {e.printStackTrace();}
			return "OK";
		}
		
		@GetMapping(value = "/hellokafka/track")
		public String initGroupByTracking() {
			//wordsKS.runStream();
			trackingKS.trackByVehicle(MAIN_TOPIC,VEHICLE_BY_TOPIC);
			trackingKS.trackByGroup(MAIN_TOPIC,GROUP_BY_TOPIC);
			trackingKS.trackByEvent(MAIN_TOPIC,EVENT_BY_TOPIC);
			return "OK";
		}
	    
	}
