package com.spboot.websocket;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Controller;

import com.spboot.service.KafkaRec;

@Controller
public class WSController {

	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaRec.class);
	
	 @Autowired
	 private SimpMessagingTemplate template;
	
	
	  //@MessageMapping("/hello")
	  //@SendTo("/topic/greetings")
	  public void sendWSMessage(String msg) {
	    String JsonMsg= "{ name :"+msg+"}";
		  
		LOGGER.info("Sending WSMessage ='{}'", msg.toString());
	    
		template.convertAndSend("/topic/greetings", msg.toString());
	  }

	  
	  public void sendWSTrackByVehicle(String msg) {
		String JsonVehicleMsg= "{ VehiclesTracking :"+msg+"}";
		  
		LOGGER.info("Sending WSMessage-TrackByVehicle ='{}'", msg.toString());
	    
		template.convertAndSend("/topic/track/byvehicle", msg.toString());
	  }
	  
	  public void sendWSTrackByGroup(String msg) {
	    String JsonGroupMsg= "{ GroupsTracking :"+msg+"}";
		  
		LOGGER.info("Sending WSMessage-TrackByGroup ='{}'", msg.toString());
	    
		template.convertAndSend("/topic/track/bygroup", msg.toString());
	  }
}
