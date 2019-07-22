package simran.ahluwalia.kafka.chapterone;

import java.util.Map;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;


public class SearchClient {

    private SearchClient() {
        throw new IllegalArgumentException("Static class");
    }

    private static Map<String, Object> configs =
        ConfigReader.getInstance().getConfigs();

    public static <E> E getSearchClient() {
        return getElasticSearchClient();
    }

    @SuppressWarnings("unchecked")
    private static <E> E getElasticSearchClient() {
        String hostName = (String) configs.get("es_hostname");
        Integer port = (Integer) configs.get("es_port");

        return  (E) new
            RestHighLevelClient(RestClient.builder(new HttpHost(hostName, port, "http")));

    }
}
