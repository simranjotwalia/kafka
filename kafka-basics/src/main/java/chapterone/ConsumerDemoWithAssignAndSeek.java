package chapterone;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoWithAssignAndSeek {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);
        String bootstrapServer = "localhost:9092";
        String topic = "simran_walia";
        String groupId = "myGroup";

        //set properties for consumer
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //Create a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        //assign and seek are used to replay data or fetch a specific message
        TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
        long offsetToReadFrom = 5L;
        //assign the partition to consumer
        consumer.assign(Arrays.asList(partitionToReadFrom));

        //after assigning do seek
        consumer.seek(partitionToReadFrom, offsetToReadFrom);
        //poll for new data
        int noOfMessagesToRead = 5;
        boolean keepOnReading = true;
        int msgsReadSoFar = 0;
        while (keepOnReading) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                msgsReadSoFar += 1;
                logger.info("Key: " + record.key() + "\n" +
                    "Value: " + record.value() + "\n" +
                    "partition: " + record.partition() + "\n" +
                    "offset: " + record.offset());
            }
            if (msgsReadSoFar >= noOfMessagesToRead) {
                keepOnReading = false;
            }
        }
    }
}
