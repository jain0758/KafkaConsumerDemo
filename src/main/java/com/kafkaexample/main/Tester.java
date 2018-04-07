package com.kafkaexample.main;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.Banner;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.kafkaexample.consumer.GenericKafkaConsumer;

@SpringBootApplication(scanBasePackages={"com.kafkaexample"})
public class Tester implements CommandLineRunner
{
	@Autowired
	@Qualifier("avroConsumer")
	private GenericKafkaConsumer consumer;
	
	public static void main(String[] args)
	{
		SpringApplication application = new SpringApplication(Tester.class);
		application.setBannerMode(Banner.Mode.OFF);
		application.run(args);
	}

	@Override
	public void run(String... args) throws Exception
	{
		consumer.consume();
	}
}
