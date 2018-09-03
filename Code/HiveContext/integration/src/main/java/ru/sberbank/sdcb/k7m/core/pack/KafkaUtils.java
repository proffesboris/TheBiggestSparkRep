package ru.sberbank.sdcb.k7m.core.pack;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

/**
 * Утилитарный класс взаимодействия с ТФС
 * с помощью Kafka
 */
public final class KafkaUtils {

    /**
     * Наименование топика сообщений в ТФС из Облака Данных
     */
    public static final String KAFKA_TOPIC_IN = "TFS.DFDC.IN";
    /**
     * Наименование топиика сообщение из ТФС в Облако Данных
     */
    public static final String KAFKA_TOPIC_OUT = "TFS.DFDC.OUT";

    private KafkaUtils() {
    }


    /**
     * Создание kafka консьюмера сообщений из ТФС
     * @param kafkaBroker брокер сообщений
     * @param groupId идентифкатор группы консьюмера
     * @return построенный консьюмер
     */
    public static KafkaConsumer<String, String> createKafkaConsumer(String kafkaBroker, String groupId) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        properties.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 262144);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 2);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList(KAFKA_TOPIC_OUT));
        return consumer;
    }

    /**
     * Создание kafka продюсера сообщение в ТФС
     * @param kafkaBroker брокер сообщений
     * @return построенный продюсер
     */
    public static KafkaProducer<String, String> createKafkaProducer(String kafkaBroker) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, "1");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 128);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 32768);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(properties);
    }
    
}
