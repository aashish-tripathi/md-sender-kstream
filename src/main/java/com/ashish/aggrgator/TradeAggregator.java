package com.ashish.aggrgator;

import java.io.IOException;
import java.util.Base64;
import java.util.Properties;
import java.util.Arrays;

import com.ashish.marketdata.avro.Trade;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;



public class TradeAggregator implements Runnable{

    private  Properties config=null;
    private KafkaStreams streams = null;

    public TradeAggregator() {
        config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "tradeAggregator-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    }

    @Override
    public void run() {
        streams = new KafkaStreams(this.createTopology(), config);
        streams.start();
        while(true){
            streams.localThreadsMetadata().forEach(data -> System.out.println(data));
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                closeStream();
                break;
            }
        }

    }
    public Topology createTopology(){
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> tradesLines = builder.stream("exsim.nse.trades");

        KTable<String, Long> tradeCountsPerSymbol = tradesLines
                .mapValues(tradeEn -> deSerealizeAvroHttpRequestJSON(Base64.getDecoder().decode(tradeEn)).getSymbol().toString())
                .selectKey((key, symbol) -> symbol)
                .groupByKey()
                .count(Materialized.as("Counts"));
        tradeCountsPerSymbol.toStream().to("trade-counts-output", Produced.with(Serdes.String(), Serdes.Long()));
        return builder.build();
    }

    public Trade deSerealizeAvroHttpRequestJSON(byte[] data) {
        DatumReader<Trade> reader
                = new SpecificDatumReader<>(Trade.class);
        Decoder decoder = null;
        try {
            decoder = DecoderFactory.get().jsonDecoder(Trade.getClassSchema(), new String(data));
            return reader.read(null, decoder);
        } catch (IOException e) {
            //logger.error("Deserialization error:" + e.getMessage());
        }
        return null;
    }

    public void closeStream(){
        streams.close();
    }
}
