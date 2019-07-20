package chapterone;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallbackWithKey {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallbackWithKey.class);
        //create producer properties
        String bootStrapServers  = "localhost:9092";
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create a producer
        KafkaProducer<String, String> producer = new KafkaProducer(props);
        for(int i=0; i<5; i++){
            String topic  = "simran_walia";
            String value = "Kidan Bai " + i;
            //Idea of key here is the records with same key go to same partition in Order.
            String key = "id_" + i;
            logger.info("Key is: " + Integer.toString(i));
            //create a record
            final ProducerRecord<String, String> record1 = new ProducerRecord<String, String>(topic, key, value);
            //send data
            producer.send(record1, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e == null){
                        logger.info("Produced a record \n" + "with metadata " +  "\n" +
                            "Topic: " + recordMetadata.topic() + "\n" +
                            "Partition: " + recordMetadata.partition() + "\n" +
                            "Offset: " + recordMetadata.offset() + "\n" +
                            "Timestamp: " + recordMetadata.timestamp() );
                    }else {

                    }
                }
            });
        }


        //flush and close producer

        producer.flush();
        producer.close();
    }
}
