package com.kafkaexample.app;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.Banner;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.kafkaexample.consumer.ConsumerInterface;

@SpringBootApplication(scanBasePackages={"com.kafkaexample"})
public class KafkaConsumerApplication implements CommandLineRunner
{
	@Autowired
	@Qualifier("avroConsumer")
	private ConsumerInterface consumer;
	
	public static void main(String[] args)
	{
		SpringApplication application = new SpringApplication(KafkaConsumerApplication.class);
		application.setBannerMode(Banner.Mode.OFF);
		application.run(args);
	}

	@Override
	public void run(String... args) throws Exception
	{
		consumer.consume();
	}
}
