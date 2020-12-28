import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * User Notification Service
 * should only get suspicious transactions
 */
public class Application {

    private static final String ST_TOPIC = "suspicious-transactions";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092, localhost:9093, localhost:9094";

    public static void main(String[] args) {

        // note kafka does not like spaces in these groups
        String consumerGroup = "suspicious-transactions-group";

        // if the user passes in a cli parameter that parameter will be used as the name of the consumer group
        if (args.length == 1) {
            consumerGroup = args[0];
        }

        // print out what consumer group the consumer is joining
        System.out.println("Consumer is part of consumer group " + consumerGroup);

        // create the consumer
        Consumer<String, Transaction> kafkaConsumer = createKafkaConsumer(BOOTSTRAP_SERVERS, consumerGroup);

        consumeMessages(ST_TOPIC, kafkaConsumer);
    }

    public static void consumeMessages(String topic, Consumer<String, Transaction> kafkaConsumer) {

        // tell kafka that this consumer wants to subscribe to this topic
        kafkaConsumer.subscribe(Collections.singletonList(topic));
        System.out.println("Record of suspicious transactions \n");

        while (true) {
            // ConsumerRecords is a collection of individual consumer records
            // kafka message as viewed from the consumer
            // poll continuously checks with the cluster to see if there is any new messages in this topic
            ConsumerRecords<String, Transaction> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));

            // check if the consumer records in empty
            if (consumerRecords.isEmpty()) {
            }
            // process messages that have been read
            for (ConsumerRecord<String, Transaction> transactionRecord : consumerRecords) {
                System.out.println(String.format("Received record(key: %s, value: %s, partition: %d, offset: %d",
                        transactionRecord.key(), transactionRecord.value(), transactionRecord.partition(), transactionRecord.offset()));

                sendUserNotification(transactionRecord.value());
                System.out.println("\n");
            }
            kafkaConsumer.commitAsync(); // tells kafka the processing of those messages is done
        }
    }

    public static Consumer<String, Transaction> createKafkaConsumer(String bootstrapServers, String consumerGroup) {
        // return a consumer
        Properties properties = new Properties();

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());//consumer
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Transaction.TransactionDeserializer.class.getName()); //transaction
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        System.out.println("Transaction consumer");
        return new KafkaConsumer<>(properties);
    }

    private static void sendUserNotification(Transaction transaction) {
        // Print transaction information to the console
        System.out.println(transaction.toString());
    }

}
