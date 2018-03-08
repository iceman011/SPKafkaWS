package  com.kafka.analytics.tracking;

import com.kafka.analytics.ser.JsonPOJOSerializer;
import com.kafka.analytics.ser.JsonPOJODeserializer;
import com.kafka.analytics.pojo.TrackingMessage;

// generic Java imports
import java.util.Properties;
import java.util.HashMap;
import java.util.Map;
// Kafka imports
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Deserializer;
// Kafka Streams related imports
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TrackingAnalytic {
    
	private static final Logger LOGGER = LoggerFactory.getLogger(TrackingAnalytic.class);

	private Serde < TrackingMessage > getTrackingSerde(){
		
	      // define trackingMessageSerde
        Map< String, Object > serdeProps = new HashMap < > ();
        final Serializer < TrackingMessage > trackingMessageSerializer = new JsonPOJOSerializer<> ();
        
        serdeProps.put("JsonPOJOClass", TrackingMessage.class);
        trackingMessageSerializer.configure(serdeProps, false);
        
 
        final Deserializer < TrackingMessage > trackingMessageDeserializer = new JsonPOJODeserializer <> ();
        serdeProps.put("JsonPOJOClass", TrackingMessage.class);
        trackingMessageDeserializer.configure(serdeProps, false);
        
        return Serdes.serdeFrom(trackingMessageSerializer, trackingMessageDeserializer);
 
	}
	
	public void trackByGroup(String in_topic,String out_topic) {
		
		LOGGER.info("TrackByGroup Start");

        // Create an instance of StreamsConfig from the Properties instance
        StreamsConfig config = new StreamsConfig(getProperties());
        
        // create serd (ser/deser) objects
        final Serde < String > stringSerde = Serdes.String();
        final Serde < Long > longSerde = Serdes.Long();
        final Serde < TrackingMessage > trackingMessageSerde = getTrackingSerde();
 
        // building Kafka Streams Model
        KStreamBuilder kStreamBuilder = new KStreamBuilder();
        
        // the source of the streaming analysis is the topic with country messages
        KStream<String, TrackingMessage> trackingStream = 
                                       kStreamBuilder.stream(stringSerde, trackingMessageSerde, in_topic);
 
        // THIS IS THE CORE OF THE STREAMING ANALYTICS:
        // running count of countries per continent, published in topic RunningCountryCountPerContinent
        KTable<String,Long> trackingPerGroup = trackingStream
                                                                 .selectKey((k, country) -> country.group_id)
                                                                 .countByKey("Counts");
        
        
        trackingPerGroup.to(stringSerde, longSerde,  out_topic);
        trackingPerGroup.print(stringSerde, longSerde);
         
 
        System.out.println("Starting Kafka Tracking by Group Streams ");
        KafkaStreams kafkaStreams = new KafkaStreams(kStreamBuilder, config);
        kafkaStreams.start();
        System.out.println("Now started Kafka Tracking by Group Streams ");

	}
	
    public static void main(String[] args) {
    	(new TrackingAnalytic()).trackByGroup("hello_topic","topic_by_group");
    	/*
        System.out.println("Kafka Streams Demonstration");

        // Create an instance of StreamsConfig from the Properties instance
        StreamsConfig config = new StreamsConfig(getProperties());
        final Serde < String > stringSerde = Serdes.String();
        final Serde < Long > longSerde = Serdes.Long();
 
        // define trackingMessageSerde
        Map< String, Object > serdeProps = new HashMap < > ();
        final Serializer < TrackingMessage > countryMessageSerializer = new JsonPOJOSerializer<> ();
        
        serdeProps.put("JsonPOJOClass", TrackingMessage.class);
        countryMessageSerializer.configure(serdeProps, false);
        
 
        final Deserializer < TrackingMessage > countryMessageDeserializer = new JsonPOJODeserializer <> ();
        serdeProps.put("JsonPOJOClass", TrackingMessage.class);
        countryMessageDeserializer.configure(serdeProps, false);
        
        final Serde < TrackingMessage > trackingMessageSerde = Serdes.serdeFrom(countryMessageSerializer, countryMessageDeserializer);
 
        // building Kafka Streams Model
        KStreamBuilder kStreamBuilder = new KStreamBuilder();
        // the source of the streaming analysis is the topic with country messages
        KStream<String, TrackingMessage> trackingStream = 
                                       kStreamBuilder.stream(stringSerde, trackingMessageSerde, "hello_topic");
 
        // THIS IS THE CORE OF THE STREAMING ANALYTICS:
        // running count of countries per continent, published in topic RunningCountryCountPerContinent
        KTable<String,Long> trackingPerGroup = trackingStream
                                                                 .selectKey((k, country) -> country.group_id)
                                                                 .countByKey("Counts");
        
        
        trackingPerGroup.to(stringSerde, longSerde,  "RunningCountryCountPerContinent");
        trackingPerGroup.print(stringSerde, longSerde);
 
        trackingStream.foreach(new ForeachAction<String,TrackingMessage>() {
			
			public void apply(String key, TrackingMessage value) {
				// TODO Auto-generated method stub
				System.out.println(" looping " +key + ": " + value);
			}
		});
        
 
        System.out.println("Starting Kafka Streams Countries Example");
        KafkaStreams kafkaStreams = new KafkaStreams(kStreamBuilder, config);
        kafkaStreams.start();
        System.out.println("Now started trackingStreams Example");*/
        
    }

    private static Properties getProperties() {
        Properties settings = new Properties();
        // Set a few key parameters
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "Tracking-App");
        // Kafka bootstrap server (broker to talk to); ubuntu is the host name for my VM running Kafka, port 9092 is where the (single) broker listens 
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // Apache ZooKeeper instance keeping watch over the Kafka cluster; ubuntu is the host name for my VM running Kafka, port 2181 is where the ZooKeeper listens 
        settings.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        // default serdes for serialzing and deserializing key and value from and to streams in case no specific Serde is specified
        settings.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        settings.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        //settings.put(StreamsConfig.STATE_DIR_CONFIG, "C:\\temp");
        // to work around exception Exception in thread "StreamThread-1" java.lang.IllegalArgumentException: Invalid timestamp -1
        // at org.apache.kafka.clients.producer.ProducerRecord.<init>(ProducerRecord.java:60)
        // see: https://groups.google.com/forum/#!topic/confluent-platform/5oT0GRztPBo
        settings.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        settings.put(StreamsConfig.STATE_DIR_CONFIG ,"/tmp");
        return settings;
    }

}