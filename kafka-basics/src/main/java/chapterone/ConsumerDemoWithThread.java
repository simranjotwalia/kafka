package chapterone;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {
    Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class);

    public static void main(String[] args) {
        ConfigReader reader = new ConfigReader();
        Map<String, Object> configs = reader.getConfigs();
        new ConsumerDemoWithThread().spawnThread(configs);
    }

    public void spawnThread(Map<String, Object> configs){

        //Declaring configs for thread and spawning a new thread
        String bootStrapServer = (String) configs.get("bootStrapServer");
        String topicName = (String) configs.get("topicName");
        String groupId = (String) configs.get("groupId");

        //setting up latch to tie it with main program
        CountDownLatch latch = new CountDownLatch(1);
        ConsumerRunnable runnable = new ConsumerRunnable(bootStrapServer, topicName, groupId,latch);

        Thread consumer = new Thread(runnable);

        consumer.start();

        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            logger.info("Inside shutdown hook");
            runnable.shutDownConsumer();

            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


    }
    public class ConsumerRunnable implements  Runnable{
        KafkaConsumer<String, String> consumer;
        CountDownLatch latch;
        Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);

        public ConsumerRunnable(String bootstrapServer, String topicName, String groupId, CountDownLatch latch){
            logger.info("Initiating thread");
            Map<String, String> propsMap = new HashMap<>();
            propsMap.put("bootstrap_server", bootstrapServer);
            propsMap.put("group_id", groupId);
            this.latch = latch;
            this.consumer = new KafkaConsumer<String, String>(getProperties(propsMap));
            //subscribe to topic
            consumer.subscribe(Arrays.asList(topicName));
        }
        @Override
        public void run() {
            try {
                //continuously polling for records
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                    for (ConsumerRecord<String, String> record: records){
                        logger.info("Key: " +  record.key() + "\n" +
                            "Value: " + record.value() + "\n" +
                            "partition: " + record.partition() + "\n" +
                            "offset: " +  record.offset());
                    }
                }
            } catch (WakeupException e) {
                //this is a safe exception. We know this would occur
                logger.info("Got into wakeup");
            }finally {
                logger.info("Closing consumer");
                consumer.close();
                latch.countDown();
                logger.info("Consumer closed");
            }
        }

        public Properties getProperties(Map<String, String> props){
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, props.get("bootstrap_server"));
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, props.get("group_id"));
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            return properties;
        }

        public void shutDownConsumer(){
            //wakeup() will interrupt consumer.poll(), it will throw an exception WakeupException
            logger.info("Shutting down consumer");
            consumer.wakeup();
        }
    }
}

