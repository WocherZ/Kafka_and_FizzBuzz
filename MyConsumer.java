import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import static java.nio.file.StandardOpenOption.CREATE;

public class MyConsumer {
    private Properties props;
    private KafkaConsumer<String, Integer> consumer;

    private int maxNumber = 1_000_000;
    MyConsumer() {
        props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test_id");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        consumer = new KafkaConsumer<>(props);
    }

    public void subscribeTopic(String topicName) {
        consumer.subscribe(List.of(topicName));
    }

    private String fizzBuzzAlgorithm(int value) {
        String result = "";
        if (value % 3 == 0) result += "fizz";
        if (value % 5 == 0) result += "buzz";
        if (result.equals("")) result += Integer.toString(value);
        return result;
    }

    public void readNumbers() {
        int value = 0;
        List<String> outputs = new ArrayList<>();
        try {
            while (true) {
                ConsumerRecords<String, Integer> records = consumer.poll(100);
                for (ConsumerRecord<String, Integer> record : records) {
                    value = record.value();
                    outputs.add(fizzBuzzAlgorithm(value));
                }
                if (value == maxNumber) {
                    Files.write(Paths.get("answer.txt"), outputs, StandardCharsets.UTF_8, CREATE);
                    break;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
