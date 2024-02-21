package com.doctorkernel.streamsstarterproject.services;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Service
@RequiredArgsConstructor
public class KafkaStreamsService {
    private final Properties streamsConfig;

    public void startKafkaStreams(Topology topology){
        KafkaStreams kafkaStreams= new KafkaStreams(topology, streamsConfig);
        kafkaStreams.start();
        System.out.println(kafkaStreams.toString());
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }
}
