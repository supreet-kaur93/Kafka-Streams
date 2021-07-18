package handler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.util.Map;

public class JsonSerializer<T> implements Serializer<T> {

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public byte[] serialize(String topic, T data) {
        byte[] serializedVal = null;

        // We can do filtering based on the class used,
        // like pick all strings and handle them separately in different topic
        System.out.println(data.getClass());
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            serializedVal = objectMapper.readValue(objectMapper.writeValueAsBytes(data), byte[].class);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            throw new SerializationException();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException();
        }
        return serializedVal;
    }

    @Override
    public void close() {

    }
}
