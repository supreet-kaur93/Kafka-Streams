package handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.xml.internal.ws.encoding.soap.DeserializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class JsonDeserializer<T> implements Deserializer<T> {

    private Class<T> type;

    public JsonDeserializer(){}

    public JsonDeserializer(Class<T> type) {
        this.type = type;
    }

    /*
    An unchecked warning tells a programmer that a cast may cause a program to throw an exception somewhere else.
    Suppressing the warning with @SuppressWarnings("unchecked") tells the compiler that the programmer
    believes the code to be safe and won't cause unexpected exceptions.
     */
    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> map, boolean b) {
        if(Objects.isNull(type)) {
            type = (Class<T>) map.get("type");
        }
    }

    @Override
    public T deserialize(String s, byte[] bytes) throws DeserializationException {
        ObjectMapper objectMapper = new ObjectMapper();
        T deserializedVal = null;
        if(Objects.isNull(bytes) || bytes.length == 0) {
            return null;
        }
        try {
            deserializedVal = objectMapper.readValue(bytes, type);
        } catch (IOException e) {
            e.printStackTrace();
            throw new DeserializationException("EXCEPTION WHILE DESERIALIZING DATA - {}", bytes);
        }
        return deserializedVal;
    }

    @Override
    public void close() {

    }

    Class<T> getType() {
        return type;
    }
}
