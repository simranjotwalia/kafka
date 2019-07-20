package simran.ahluwalia.kafka.chapterone;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticSearchConsumer {

    Map<String, Object> configs = ConfigReader.getInstance().getConfigs();
    Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class);

    public static void main(String[] args) {
        ElasticSearchConsumer esConsumer = new ElasticSearchConsumer();
        RestHighLevelClient esClient =  esConsumer.getESClient();
        //String doc = esConsumer.getDocJson();
        //esConsumer.publishDoc(doc, esClient);

        KafkaConsumer kafkaConsumer = esConsumer.getKafkaConsumer();
        esConsumer.indexTweets(kafkaConsumer, esClient);
        esConsumer.closeClient(esClient);


    }

    private void indexTweets(KafkaConsumer consumer, RestHighLevelClient esClient) {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                publishDoc(record.value(), esClient);
            }
        }
    }

    private String getDocJson() {
        String json = "{\"name\": \"simran\", \"last\": \"walia\"}";
        return json;
    }

    private <E> void publishDoc(E doc, RestHighLevelClient client) {
        IndexRequest indexRequest = new IndexRequest(
            (String) configs.get("es_index_name"),
            (String) configs.get("es_index_type")
        ).source(doc, XContentType.JSON);

        try {
           IndexResponse response = client.index(indexRequest);
            logger.info(response.getId());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    private RestHighLevelClient getESClient() {
        String hostName = (String) configs.get("es_hostname");
        Integer port = (Integer) configs.get("es_port");

        RestHighLevelClient restClient = new
            RestHighLevelClient(RestClient.builder(new HttpHost(hostName, port, "http")));

        return restClient;
    }

    private void closeClient(RestHighLevelClient client) {
        try {
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private KafkaConsumer getKafkaConsumer() {
        String bootstrapServer = (String) configs.get("bootStrapServer");
        String topic = (String) configs.get("topic_twitter");
        String groupId = (String) configs.get("twitter_es_groupId");

        //set properties for consumer
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //Create a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        //subscribe to the topics
        consumer.subscribe(Arrays.asList(topic));
        //poll for new data

        return consumer;
    }
}
