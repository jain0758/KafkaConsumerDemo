package com.main;

import org.springframework.boot.Banner;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.consumer.MyKafkaConsumer;

@SpringBootApplication
public class Tester implements CommandLineRunner
{
	public static void main(String[] args)
	{
		SpringApplication application = new SpringApplication(Tester.class);
		application.setBannerMode(Banner.Mode.OFF);
		application.run(args);
	}

	@Override
	public void run(String... args) throws Exception
	{
		new MyKafkaConsumer().consumeData();
	}
}
