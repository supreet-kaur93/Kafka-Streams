package handler;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public class PropertiesHandler {

     public static Properties getProperties(final String bootstrapServer, String applicationServerConfig) {
        /*
        To specify which serializers/deserializers to use by default, and to specify security settings
         */

        final Properties streamConfig = new Properties();
        /*
        Required Parameters - application.id = An Identifier for stream processing app within cluster
        bootstrap.servers - A list of host/port pairs to use for establishing the initial connection to Kafka cluster
        This is unique in Kafka Cluster

        There are few optional fields available as well like - DeserializationExceptionHandler (logs deserialization exception)
        There are two such exception handlers available(LogAndContinueExceptionHandler, LogAndFailExceptionHandler.),
        ProductionExceptionHandler (Handles exceptions triggered when trying to interact with broker),

        key.serde for Default serializer/deserializer class for record keys, implements the Serde interface (only if done via StreamsBuilder#stream() or KStream#to())
        Similarly value.serde for Default serializer/deserializer class for record value
        */

        /*
        When application is updated, application.id should be changed unless we want to reuse the existing data in
        internal topic and data stores
         */

        streamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-cluster");
        streamConfig.put(StreamsConfig.CLIENT_ID_CONFIG, "kafka-cluster-client"); // Default Kafka producer and consumer prefix
        streamConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        streamConfig.put(StreamsConfig.APPLICATION_SERVER_CONFIG, applicationServerConfig);
        streamConfig.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams");

        /*
        Consumer Property
        Reading events from earliest to avoid missing
        We can set consumer session timeout as well
         */
        streamConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // To handle special production exceptions
        // streamConfig.put(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG, IgnoreRecordTooLargeHandler.class);

        // To capture timestamp of record creation/consumption
        streamConfig.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, EventTimeExtractorHandler.class);

        // Rockdb is used as default storage engine for persistent stores
        streamConfig.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, CustomRockDbConfig.class);

        // internal topics replication factor when local states are used or streams are repartitioned for aggregation
        // Increase the replication factor to 3 to ensure that the internal Kafka Streams topic can tolerate up to 2 broker failures
        streamConfig.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);
        streamConfig.put(StreamsConfig.producerPrefix(ProducerConfig.ACKS_CONFIG), "all");

        return streamConfig;

    }

}
