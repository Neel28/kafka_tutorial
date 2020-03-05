package kafka.tutorial1;

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
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {
    public static void main(String[] args) {

        new ConsumerDemoWithThread().run();

    }

    private ConsumerDemoWithThread(){

    }
     private void run(){
         Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());
         String bootstrapServers = "127.0.0.1:9092";
         String groupId = "my-fourth-application";
         String topic = "first_topic";

         // latch for dealing multiple thread
         CountDownLatch latch = new CountDownLatch(1);

         //create the consumer runnable
         logger.info("creating the consumer thread!");
         Runnable myConsumerRunnable = new ConsumerRunnable(latch, bootstrapServers, groupId, topic);

         //start the thread
         Thread myThread = new Thread(myConsumerRunnable);
         myThread.start();

         //add a shutdown hook
         Runtime.getRuntime().addShutdownHook(new Thread( () -> {
             logger.info("caught shutdown hook");
             ((ConsumerRunnable) myConsumerRunnable).shutdown();
             try {
                 latch.await();
             }catch (InterruptedException e){
                 logger.error("application got interrupted", e);
             }
             logger.info("application has exited");
         }

         ));

         try {
             latch.await();
         }catch (InterruptedException e){
             logger.error("application got interrupted", e);
         }finally {
             logger.info("application is closing");
         }
     }

     public class ConsumerRunnable implements Runnable {

        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());

        public ConsumerRunnable(CountDownLatch latch,
                              String bootstrapServers,
                              String groupId,
                              String topic) {
            this.latch = latch;

            //create consumer configs
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            //create consumer
            KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

            //subscribe consumer to our topic(s)
            consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            //poll for new data
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("key: " + record.key() + ", value: " + record.value());
                        logger.info("partition: " + record.partition() + ", offset: " + record.offset());
                    }
                }
            }
            catch (WakeupException e){
                logger.info("received shutdown signal!");
            }
            finally {
                consumer.close();
                //tell our main code we are done with the consumer
                latch.countDown();
            }
        }

        public void shutdown(){
            // wakeup() method is a special method to interrupt consumer.poll()
            // it will throw the exception WakeUpException
            consumer.wakeup();
        }
    }
}
