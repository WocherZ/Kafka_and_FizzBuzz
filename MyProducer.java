import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class MyProducer {
    private String server = "localhost:9092";
    private String topicName;
    private Producer<String, Integer> producer;

    private Properties props;

    private int maxNumber = 1_000_000;

    MyProducer(String topicName) {
        this.topicName = topicName;
        props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                server);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                IntegerSerializer.class.getName());
        props.put("transactional.id", "my-transactional-id");
        producer = new KafkaProducer<>(props);
    }

    public void sendNumber(Integer value) throws ExecutionException, InterruptedException {
        producer.send(new ProducerRecord<>(topicName, value)).get();
    }

    public void sendMillionNumbers() throws ExecutionException, InterruptedException {
        producer.initTransactions();
        try {
            producer.beginTransaction();
            for (int i = 1; i <= maxNumber; i++) sendNumber(i);
        } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
            producer.close();
        } catch (KafkaException e) {
            producer.abortTransaction();
        } finally {
            producer.close();
        }
    }
}
