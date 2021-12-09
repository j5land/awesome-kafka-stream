package com.j5land.wordcount.kafka.stream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;

/**
 * @author Herry Jiang
 * @date 2021/12/9
 */
@Component
public class StreamSample implements ApplicationListener {

    private static final String INPUT_TOPIC = "worldcount.in";
    private static final String OUTPUT_TOPIC = "worldcount.stream.out";

    private boolean started = false;

    @Override
    public void onApplicationEvent(ApplicationEvent applicationEvent) {
        if (started){
            return;
        }
        started = true;
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.101.89:9092,192.168.100.52:9092,192.168.101.90:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "world-count");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        //构建流结构拓扑
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        wordCountStream(streamsBuilder);

        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);
        kafkaStreams.start();
    }

    private void wordCountStream(StreamsBuilder streamsBuilder){
        KStream<String, String> kStream = streamsBuilder.stream(INPUT_TOPIC);
        //hello world count
        KTable<String, Long> kTable = kStream.flatMapValues(s -> Arrays.asList(s.toLowerCase(Locale.getDefault()).split(" ")))
                .groupBy((key, value) -> value)
                .count();
        kTable.toStream().to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));
    }


    /**
     *
     //生产者控制台
     ./bin/kafka-console-producer.sh --broker-list 192.168.101.89:9092,192.168.100.52:9092,192.168.101.90:9092 --topic ads.pomo.tag
     //消费者控制台
     ./bin/kafka-console-consumer.sh --bootstrap-server 192.168.101.89:9092,192.168.100.52:9092,192.168.101.90:9092 \
     --topic events.ads.stream.total \
     --property print.key=true \
     --property print.value=true \
     --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
     --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer \
     --from-beginning
     **/

    //world-count-KSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition
}
