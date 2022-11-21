import java.util.concurrent.ExecutionException;

public class MainProducer {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String topicName = "FizzBuzz";
        MyProducer producer = new MyProducer(topicName);
        producer.sendMillionNumbers();
    }
}