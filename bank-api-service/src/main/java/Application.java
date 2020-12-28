import jdk.javadoc.doclet.Reporter;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Banking API Service
 * needs to produce messages to the topics
 */
public class Application {

    // suspicious-transactions with 2 partitions + replication factor of 3
    private static final String ST_TOPIC = "suspicious-transactions";
    // valid-transactions with 3 partitions + replication factor of 3
    private static final String VT_TOPIC = "valid-transactions";
    private static final String HVT_TOPIC = "high-value-transactions";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092, localhost:9093, localhost:9094"; // 3 brokers


    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // create the producer
        Producer<String, Transaction> kafkaProducer = createKafkaProducer(BOOTSTRAP_SERVERS);

        IncomingTransactionsReader user = new IncomingTransactionsReader();
        CustomerAddressDatabase transactionLocation = new CustomerAddressDatabase();

        try {
            processTransactions(user, transactionLocation, kafkaProducer);
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            // clean out the producer of any messages that are unsent or processing
            // flushing out the connection between kafka and the producer
            kafkaProducer.flush();
            // close any connections that were formed
            kafkaProducer.close();
        }
        System.out.println("process transactions");
    }

    public static void processTransactions(IncomingTransactionsReader incomingTransactionsReader,
                                           CustomerAddressDatabase customerAddressDatabase,
                                           Producer<String, Transaction> kafkaProducer) throws ExecutionException, InterruptedException {
        /*
         * Retrieve the next transaction from the IncomingTransactionsReader
         * For the transaction user, get the user residence from the UserResidenceDatabase
         * Compare user residence to transaction location.
         * Send a message to the appropriate topic, depending on whether the user residence and transaction
         * location match or not.
         * Print record metadata information
         * - output example
         * Record with key: joe1680, value: Transaction{user='joel680', ammount=128.63, transactionLoaction='Ireland'})
         * was sent to (partition 2, offset: 57, topic: valid-transaction)
         */

        int partition = 1;

        // loop to generate different keys and values
        while (incomingTransactionsReader.hasNext()) {
            Transaction transaction = incomingTransactionsReader.next();
            String userResidence = customerAddressDatabase.getUserResidence(transaction.getUser());
            String id = transaction.getUser();

            // high value transactions
            if (transaction.getAmount() > 1000) {
                ProducerRecord<String, Transaction> transactionRecord =
                        new ProducerRecord<>(HVT_TOPIC, id, transaction);

                RecordMetadata highvTranMetadata = kafkaProducer.send(transactionRecord).get();
                System.out.println(String.format("Record from (key: %s, value: %s) was sent to (partition: %d, offset: %d",
                        transactionRecord.key(), transactionRecord.value(), highvTranMetadata.partition(), highvTranMetadata.offset()));
                System.out.println("\n");
            }
            // valid transactions
            if (userResidence.equalsIgnoreCase(transaction.getTransactionLocation())) {

                // built in Kafka class in the API
                // The producer needs to know what type of record this is
                ProducerRecord<String, Transaction> transactionRecord =
                        new ProducerRecord<>(VT_TOPIC, id, transaction);

                // Kafka returns info where it got written to | returns in an object called RecordMetadata
                RecordMetadata validTranMetadata = kafkaProducer.send(transactionRecord).get(); // sending a record 'transaction' to Kafka and getting the result
                // print out each record and the Metadata Kafka returned | the offset shows the ordering in the partition
                System.out.println(String.format("Record from (key: %s, value: %s) was sent to (partition: %d, offset: %d",
                        transactionRecord.key(), transactionRecord.value(), validTranMetadata.partition(), validTranMetadata.offset()));
                System.out.println("\n");
            }
            // suspicious tranactions
            else if (!(userResidence.equalsIgnoreCase(transaction.getTransactionLocation()))) {
                ProducerRecord<String, Transaction> transactionRecord =
                        new ProducerRecord<>(ST_TOPIC, id, transaction);

                RecordMetadata suspiciousTranMetadata = kafkaProducer.send(transactionRecord).get();
                System.out.println(String.format("Record from (key: %s, value: %s) was sent to (partition: %d, offset: %d",
                        transactionRecord.key(), transactionRecord.value(), suspiciousTranMetadata.partition(), suspiciousTranMetadata.offset()));
                System.out.println("\n");
            }
            System.out.println("Record of transaction \n");
        }
    }

    public static Producer<String, Transaction> createKafkaProducer(String bootstrapServers) {
        // return a producer
        Properties properties = new Properties();

        // connect to the bootstrap servers
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "transaction-producer");
        /*
         * Any object can be used as a key | any object can be used as a value
         * Telling Kafka how to serialize these objects how to convert these objects into bit streams
           that can transmit across the network and then reform at the other side.
        */
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // producer
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Transaction.TransactionSerializer.class.getName()); //transaction

        System.out.println("Transaction producer");
        return new KafkaProducer<String, Transaction>(properties);
    }

}
