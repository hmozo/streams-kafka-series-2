package com.doctorkernel.streamsstarterproject.config;

import com.doctorkernel.streamsstarterproject.services.KafkaStreamsService;
import com.doctorkernel.streamsstarterproject.services.WordcountStreamsService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import java.util.Map;
import java.util.Properties;

@Configuration
public class KafkaConfig {
    @Autowired
    Environment environment;
    @Bean
    public Properties streamsConfig(){
        var map= Map.<String, Object>of(
                StreamsConfig.APPLICATION_ID_CONFIG, environment.getProperty("spring.kafka.streams.application-id-config"),
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, environment.getProperty("spring.kafka.streams.bootstraps-servers-config"),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, environment.getProperty("spring.kafka.consumer.auto-offset-reset-config"),
                StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass(),
                StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass(),
                StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0 // only for DEV
                );
        Properties properties= new Properties();
        for(var entry:map.entrySet()){
            properties.put(entry.getKey(), entry.getValue());
        }
        return properties;
    }
    @Bean
    public KafkaStreamsService kafkaStreamsService(){
        return new KafkaStreamsService(streamsConfig());
    }

}
