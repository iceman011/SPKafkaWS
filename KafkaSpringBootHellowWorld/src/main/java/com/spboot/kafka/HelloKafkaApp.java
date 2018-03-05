package com.spboot.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;


@SpringBootApplication
@ComponentScan({ "com.spboot.controller" })
public class HelloKafkaApp {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SpringApplication.run(
				new Object[] { HelloKafkaApp.class }, args);

	}

}
