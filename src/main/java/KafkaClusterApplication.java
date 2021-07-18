import handler.ConstantStrings;
import handler.PropertiesHandler;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Properties;

public class KafkaClusterApplication {

    /*
    Kafka Streams is a client library for building applications and microservices, where the input and output data are stored in Kafka clusters.
     messages are key and value
     */

    /*
    Streams DSL(Domain Specific Language) - is build on top of Streams Processor API.
     1. Build in abstractions for streams and tables inform of KStream, KTable and GlobalKTable
     2. Declarative functional programming style with stateless transformations (like map and filter)
     as well as stateful transformations such as aggregations, joins, windowing(session window)
     3. Processor topologies - logical processing plan
     4. No separate processing cluster required
     */

    public static void main(String[] args) {

        /*
        Get the Kafka Server
         */
        final String bootstrapServer = args.length > 0 ? args[0] : "localhost:6062";

        /*
        using builders to define the actual processing topology, example to specify from which input topics to read,
        which stream operations (map, filter) should be called
         */
        StreamsBuilder builder = new StreamsBuilder();
        Topology topology = builder.build();

        Properties streamConfig = PropertiesHandler.getProperties(bootstrapServer, "localhost:9092");

        // Get stream for first topic
        // creates a KStream from specified kafka inputs and interprets the data as a record stream.
        // kstream represents a partitioned record stream
        final KStream<String, String> ordersStream = builder.stream(ConstantStrings.TOPIC);



        KStream<String, Long> wordCounts = builder.stream(
                "word-counts-input-topic", /* input topic */
                Consumed.with(
                        Serdes.String(), /* key serde */
                        Serdes.Long()   /* value serde */
                ));
        /*
        Read the specified Kafka Input topic into a KTable, the topic is interpreted as a changelog system, where records
        with same key are interpreted as UPSERT aka INSERT/UPDATE
        Data will be populated only from the subset of partitions
         */

        /*
        Reads the specified Kafka input topic into a GlobalKTable. The topic is interpreted as a changelog stream,
        where records with the same key are interpreted as UPSERT aka INSERT/UPDATE
        In the case of a GlobalKTable, the local GlobalKTable instance of every application instance will be populated
        with data from all the partitions of the input topic.
         */

        GlobalKTable<String, Long> wordCountTable = builder.globalTable(
                "word-counts-input-topic",
                Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(
                        "word-counts-global-store" /* table/store name */)
                        .withKeySerde(Serdes.String()) /* key serde */
                        .withValueSerde(Serdes.Long()) /* value serde */
        );

        /*
        Since KStream and KTable are strongly typed, all of these transformation operations are defined as
        generic functions where users could specify the input and output data types.

         Some KStream transformations may generate one or more KStream objects, for example: -
         filter and map on a KStream will generate another KStream - branch on KStream can generate multiple KStreams

         Some others may generate a KTable object, for example an aggregation of a KStream also yields a KTable.
         */

        KafkaStreams kafkaStreams = new KafkaStreams(topology, streamConfig);
        // start stream thread
        // Incase there is already a task running, it is reassigned to run on this stream
        kafkaStreams.start();

        kafkaStreams.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {

            }
        });

    }

    // close stream threads
    private void shutDownStream(KafkaStreams kafkaStreams) {
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }

}
