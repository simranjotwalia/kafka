package simran.ahluwalia.kafka.chapterone;

import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.Map;

public class ConfigReader {

    private static ConfigReader configReader = null;

    private ConfigReader(){

    }

    /**
     * Getting instance of Config reader.
     * @return Config reader
     */
    public static ConfigReader getInstance() {
        if (configReader == null) {
            configReader = new ConfigReader();
        }

        return configReader;
    }

    /**
     * Returns config map.
     * @return Config map instance.
     */
    public Map<String, Object> getConfigs() {
        Yaml yaml = new Yaml();
        InputStream stream = this.getClass().getClassLoader().getResourceAsStream("properties.yaml");
        Map<String, Object> configs = yaml.load(stream);
        return configs;
    }

}
