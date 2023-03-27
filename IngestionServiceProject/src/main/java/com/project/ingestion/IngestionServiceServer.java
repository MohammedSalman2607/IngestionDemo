package com.project.ingestion;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class IngestionServiceServer {

	public static void main(String[] args) {


		SpringApplication.run(IngestionServiceServer.class, args);

	}

}
