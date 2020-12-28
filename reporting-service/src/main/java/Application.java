import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

/**
 * Reporting Service
 * should be getting all transactions
 */
public class Application {

    // using an array list so the size of the array cannot be modified
    public static final List<String> TOPICS = new ArrayList<>(Arrays.asList("high-value-transactions", "valid-transactions", "suspicious-transactions"));
    private static final String BOOTSTRAP_SERVERS = "localhost:9092, localhost:9093, localhost:9094";

    public static void main(String[] args) {
        String consumerGroup = "transactions-group";
        if (args.length == 1) {
            consumerGroup = args[0];
        }

        System.out.println("Consumer is part of consumer group " + consumerGroup);
        Consumer<String, Transaction> kafkaConsumer = createKafkaConsumer(BOOTSTRAP_SERVERS, consumerGroup);
        consumeMessages(TOPICS, kafkaConsumer);
    }

    public static void consumeMessages(List<String> topics, Consumer<String, Transaction> kafkaConsumer) {
        kafkaConsumer.subscribe(topics);
        System.out.println("Record of all transactions \n");

        while (true) {
            ConsumerRecords<String, Transaction> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));

            if (consumerRecords.isEmpty()) {
            }
            for (ConsumerRecord<String, Transaction> transactionRecord : consumerRecords) {
                System.out.println(String.format("Received record(key: %s, value: %s, partition: %d, offset: %d",
                        transactionRecord.key(), transactionRecord.value(), transactionRecord.partition(), transactionRecord.offset()));

                recordTransactionForReporting(transactionRecord.topic(), transactionRecord.value());
            }
            kafkaConsumer.commitAsync();
        }
    }

    public static Consumer<String, Transaction> createKafkaConsumer(String bootstrapServers, String consumerGroup) {
        Properties properties = new Properties();

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());//consumer
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Transaction.TransactionDeserializer.class.getName()); //transaction
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        System.out.println("Transaction consumer");
        return new KafkaConsumer<>(properties);
    }

    private static void recordTransactionForReporting(String topic, Transaction transaction) {
        // Print transaction information to the console
        // Print a different message depending on whether transaction is suspicious or valid

        System.out.println(transaction.toString());

        // valid transactions
        if (topic.equalsIgnoreCase("valid-transactions")) {
            System.out.println("\nvalid transaction from ");
            System.out.println(transaction.toString());
        }
        // suspicious transactions
        else if (topic.equalsIgnoreCase("suspicious-transactions")) {
            System.out.println("\nsuspicious transaction from");
            System.out.println(transaction.toString());
        }
        // high value transactions
        else if (topic.equalsIgnoreCase("high-value-transactions")) {
            System.out.println("\nhigh value transaction from ");
            System.out.println(transaction.toString());
        }
    }

}
