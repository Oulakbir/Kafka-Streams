package ma.enset;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;

import java.util.Properties;

public class Main {
    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("application.id", "tp2-kafka-streams-app");
        props.put("bootstrap.servers", "localhost:9092");
        props.put("default.key.serde", Serdes.String().getClass());
        props.put("default.value.serde", Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String,String> ordersStream= builder.stream("orders");
        KStream<String,String>filteredOrdersStream=ordersStream.filter((key,value)->{
            System.out.println("key="+key+",value="+value);
            double total = Double.parseDouble(value.split(",")[1]);
            return (total>100);
        });
        KGroupedStream<String,String> groupedStream=filteredOrdersStream.groupBy((key,value)->value.split(",")[0]);
        KTable<String,Double> totalByCustomer= groupedStream.aggregate(()->0.0,(key, value, somme)->{
           double total = Double.parseDouble(value.split(",")[1]);
           return somme+total;
        }, Materialized.with(Serdes.String(),Serdes.Double()));
        totalByCustomer.toStream().to("customer-total");
        KafkaStreams streams = new KafkaStreams(builder.build(),props);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}