package chapterone;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
        //create producer properties
        String bootStrapServers  = "localhost:9092";
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create a producer
        KafkaProducer<String, String> producer = new KafkaProducer(props);
        //create a record
        ProducerRecord<String, String> record1 = new ProducerRecord<String, String>("simran_walia", "Kidan Bai");
        //send data
        producer.send(record1);

        //flush and close producer

        ProducerRecord<String, String> record2 = new ProducerRecord<String, String>("simran_walia", "bas vadia");
        producer.send(record2);
        producer.flush();
        producer.close();
    }
}
