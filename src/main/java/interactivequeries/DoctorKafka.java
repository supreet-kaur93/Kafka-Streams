package interactivequeries;

import handler.PropertiesHandler;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.HostInfo;
import services.DoctorKafkaService;

import java.util.Properties;

public class DoctorKafka {

    private static final String DEFAULT_ENDPOINT_HOSTNAME = "localhost";
    private static final String DEFAULT_BOOTSTRAP_SERVER = "localhost:9092";
    private static final String DEFAULT_SCHEMA_REGISTRY_URL = "http://localhost:8081";

    public static void main(final String[] args) {
        if(!isValidArgument(args)) {
            throw new IllegalArgumentException("ILLEGAL_ARGUMENTS, Argument 1 - rest end point port," +
                    "Argument 2 - bootstrap servers, Argument 3 - schema registory url, " +
                    "Argument 4 - End Point host");
        }

        int endpointPort = Integer.parseInt(args[0]);
        String bootstrapServer = args.length > 1 ? args[1] : DEFAULT_BOOTSTRAP_SERVER;
        String schemaRegistryUrl = args.length > 2 ? args[2] : DEFAULT_SCHEMA_REGISTRY_URL;
        String endPointHost = args.length > 3 ? args[3] : DEFAULT_ENDPOINT_HOSTNAME;

        // User defined end point in Kafka Stream
        HostInfo hostInfo = new HostInfo(endPointHost, endpointPort);

        System.out.println("REST endpoint at http://" + endPointHost + ":" + endpointPort);

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        Topology topology = streamsBuilder.build();
        Properties streamConfig = PropertiesHandler.getProperties(bootstrapServer, endPointHost + ":" +endpointPort);

        KafkaStreams kafkaStreams = new KafkaStreams(topology, streamConfig);
        kafkaStreams.cleanUp();
        kafkaStreams.start();

       // get doctor kafka object
        DoctorKafkaService doctorKafkaService = new DoctorKafkaService(kafkaStreams, hostInfo);

    }

    private static boolean isValidArgument(final String[] args) {
        return (args.length > 0 && args.length <=4);
    }

}
