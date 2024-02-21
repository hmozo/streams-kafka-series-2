package com.doctorkernel.streamsstarterproject;

import com.doctorkernel.streamsstarterproject.services.KafkaStreamsService;
import com.doctorkernel.streamsstarterproject.services.WordcountStreamsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class StreamsStarterProjectApplication implements CommandLineRunner {
    @Autowired
    private KafkaStreamsService kafkaStreamsService;
    @Autowired
    private WordcountStreamsService wordcountStreamsService;

    public static void main(String[] args) {
		SpringApplication.run(StreamsStarterProjectApplication.class, args);
	}

    @Override
    public void run(String... args) throws Exception {
        kafkaStreamsService.startKafkaStreams(wordcountStreamsService.buildTopologyFavouriteColor());
    }
}
