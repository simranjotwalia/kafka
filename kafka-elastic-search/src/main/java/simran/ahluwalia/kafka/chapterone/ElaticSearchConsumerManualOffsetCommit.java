package simran.ahluwalia.kafka.chapterone;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElaticSearchConsumerManualOffsetCommit {

    Map<String, Object> configs = ConfigReader.getInstance().getConfigs();
    Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class);

    public static void main(String[] args) {

        ElaticSearchConsumerManualOffsetCommit esApp = new ElaticSearchConsumerManualOffsetCommit();
        RestHighLevelClient esClient = SearchClient.getSearchClient();
        KafkaConsumer consumer = esApp.getKafkaConsumer();
        esApp.publishToElastic(esClient, consumer);
    }

    private void publishToElastic(RestHighLevelClient esClient, KafkaConsumer consumer) {
        logger.info("Starting consuming records....");

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(2000));
            logger.info("No of records polled {}", records.count());

            for (ConsumerRecord<String, String> record : records) {
                insert(record, esClient);
            }

            logger.info("Commiting offsets...");
            consumer.commitSync();
            logger.info("Offsets commited...");
        }


    }

    private void insert(ConsumerRecord record, RestHighLevelClient esClient) {
        //generating unique ID tp be provided to ES
        String id = record.topic() + "_" + record.partition() + "_" + record.offset();
        IndexRequest indexRequest = new IndexRequest(
            (String) configs.get("es_index_name"),
            (String) configs.get("es_index_type"),
            id
        ).source(record.value(), XContentType.JSON);

        try {
            IndexResponse response = esClient.index(indexRequest);
            logger.info("Inserted in ES with id {} ",response.getId());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    private KafkaConsumer getKafkaConsumer() {
        String bootstrapServer = (String) configs.get("bootStrapServer");
        String topic = (String) configs.get("topicName");
        String groupId = (String) configs.get("groupId");

        //set properties for consumer
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "5");

        //Create a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        //subscribe to the topics
        consumer.subscribe(Arrays.asList(topic));
        //poll for new data

        return consumer;
    }


}
