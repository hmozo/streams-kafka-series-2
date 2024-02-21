package com.doctorkernel.streamsstarterproject.services;

import com.doctorkernel.streamsstarterproject.utils.AppConstants;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.function.Function;

@Service
@RequiredArgsConstructor
public class WordcountStreamsService {
    public Topology buildTopologyWordCount(){
        Function<KStream, KTable> topologyFunction= (inputStreams)->inputStreams
                .mapValues(value->value.toString().toLowerCase())
                .flatMapValues(value-> Arrays.asList(value.toString().split(" ")))
                .selectKey((key, value)-> value)
                .groupByKey()
                .count();
        return buildTopology(AppConstants.WORD_COUNT_INPUT_TOPIC, topologyFunction, AppConstants.WORD_COUNT_OUTPUT_TOPIC);
    }

    public Topology buildTopologyFavouriteColor(){
        StreamsBuilder streamsBuilder= new StreamsBuilder();

        KStream<String, String> inputStreams= streamsBuilder.stream(AppConstants.FAVOURITE_COLOR_INPUT_TOPIC)
                .filter((key,value)->value.toString().contains(":"))
                .selectKey((key,value)->value.toString().split(":")[0].toLowerCase())
                .mapValues(value->value.toString().split(":")[1].toLowerCase())
                .filter((user,color)->Arrays.asList("green", "red", "blue").contains(color));
        inputStreams.to(AppConstants.USER_KEYS_AND_COLORS_TOPIC);

        KTable<String, String> table= streamsBuilder.table(AppConstants.USER_KEYS_AND_COLORS_TOPIC);
        KTable<String, Long> tableMod= table
            .groupBy((user,color)->new KeyValue<String, String>(color,color))
                .count();
        tableMod.toStream().to(AppConstants.FAVOURITE_COLOR_OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));
        return streamsBuilder.build();
    }

    private Topology buildTopology(String inputTopic, Function<KStream, KTable> topologyFunction, String outputTopic){
        StreamsBuilder streamsBuilder= new StreamsBuilder();
        KStream<String, String> inputStreams= streamsBuilder.stream(inputTopic);
        KTable outputTable= topologyFunction.apply(inputStreams);
        outputTable.toStream().to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));
        return streamsBuilder.build();
    }
}
