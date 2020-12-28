import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * Account Manager
 * should only be getting valid transactions
 */
public class Application {

    private static final String VT_TOPIC = "valid-transactions";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092, localhost:9093, localhost:9094";

    public static void main(String[] args) {
        String consumerGroup = "valid-transactions-group";
        if (args.length == 1) {
            consumerGroup = args[0];
        }

        System.out.println("Consumer is part of consumer group " + consumerGroup);
        Consumer<String, Transaction> kafkaConsumer = createKafkaConsumer(BOOTSTRAP_SERVERS, consumerGroup);
        consumeMessages(VT_TOPIC, kafkaConsumer);
    }

    public static void consumeMessages(String topic, Consumer<String, Transaction> kafkaConsumer) {
        kafkaConsumer.subscribe(Collections.singletonList(topic));
        System.out.println("Record of valid transactions \n");

        while (true) {
            ConsumerRecords<String, Transaction> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));

            if (consumerRecords.isEmpty()) {
            }
            for (ConsumerRecord<String, Transaction> transactionRecord : consumerRecords) {
                System.out.println(String.format("Received record(key: %s, value: %s, partition: %d, offset: %d",
                        transactionRecord.key(), transactionRecord.value(), transactionRecord.partition(), transactionRecord.offset()));
                approveTransaction(transactionRecord.value());
                System.out.println("\n");
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

    private static void approveTransaction(Transaction transaction) {
        // Print transaction information to the console
        System.out.println(transaction.toString());
    }

}
