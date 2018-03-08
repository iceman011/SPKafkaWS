/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kafka.analytics.pojo;


import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.streams.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;

public class TrackingMessage {

    
	private static final Logger LOGGER = LoggerFactory.getLogger(TrackingMessage.class);
    
	public String timestamp;
    public String vehicle_id;
    public String group_id;
    public String event;
    public String location;
    public String message;

    
    

    public String getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}

	public String getGroup_id() {
		return group_id;
	}

	public void setGroup_id(String group_id) {
		this.group_id = group_id;
	}

	public String getEvent() {
		return event;
	}

	public void setEvent(String event) {
		this.event = event;
	}

	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}
	
    String getVehicle_id()
    {
    	return vehicle_id;
    }
    
    void setVehicle_id(String v) {
    	vehicle_id = v;
    }
    
    @Override
    public String toString() {
        return "timestamp > "+timestamp+" ,vehicle_id > "+vehicle_id+ ", group_id > "+ group_id+" , event > "+event+ " , location > "+ location+ " , messssagee >"+ message;
    }

    public static KeyValue<String, TrackingMessage> parseMessage(String time, JsonNode jsonMessage) {
        try {
        	LOGGER.info("before Parsed Message "+time+" <> "+jsonMessage.asText());
            TrackingMessage msg = TrackingMessage.parseText(jsonMessage.asText());
            //System.out.println("Parsed Message "+msg );
            LOGGER.info("Parsed Message "+msg);
            return new KeyValue<>(msg.vehicle_id, msg);
        } catch (IllegalArgumentException e) {
        	//e.printStackTrace();
            return new KeyValue<>(null, null);
        }
    }

    
    public static boolean filterNonNull(String key, TrackingMessage value) {
        return key != null && value != null;
    }


    private static TrackingMessage parseText(String raw) {
        Pattern p = Pattern.compile("\\[\\[(.*)\\]\\]\\s(.*)\\s(.*)\\s\\*\\s(.*)\\s\\*\\s\\(\\+?(.\\d*)\\)\\s(.*)");
        Matcher m = p.matcher(raw);

        if (!m.find()) {
            throw new IllegalArgumentException("Could not parse message: " + raw);
        } else if (m.groupCount() != 6) {
            throw new IllegalArgumentException("Unexpected parser group count: " + m.groupCount());
        } else {
            TrackingMessage result = new TrackingMessage();

            result.timestamp = m.group(1);
            result.vehicle_id = m.group(2);
            result.group_id = m.group(3);
            result.event = m.group(4);
            result.location = m.group(5);
            result.message = m.group(6);

            return result;
        }
    }

}
