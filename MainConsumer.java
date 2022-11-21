public class MainConsumer {
    public static void main(String[] args) {
        String topicName = "FizzBuzz";
        MyConsumer consumer = new MyConsumer();
        consumer.subscribeTopic(topicName);
        consumer.readNumbers();
    }
}