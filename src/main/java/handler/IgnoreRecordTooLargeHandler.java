package handler;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;

import java.util.Map;
import java.util.Properties;

public class IgnoreRecordTooLargeHandler implements ProductionExceptionHandler {
    @Override
    public ProductionExceptionHandlerResponse handle(ProducerRecord<byte[], byte[]> producerRecord, Exception e) {
        if(e instanceof RecordTooLargeException) {
            // Continue incase size of payload is large
            return ProductionExceptionHandlerResponse.CONTINUE;
        } else {
            return ProductionExceptionHandlerResponse.FAIL;
        }
    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
