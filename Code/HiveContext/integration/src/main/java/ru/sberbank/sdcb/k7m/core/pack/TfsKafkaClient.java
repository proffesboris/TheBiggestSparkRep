package ru.sberbank.sdcb.k7m.core.pack;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;

public class TfsKafkaClient {

    private static final long HALF_HOUR_TIMEOUT = 30 * 60L;

    private KafkaConsumer<String, String> consumer;
    private KafkaProducer<String, String> producer;

    public TfsKafkaClient(String kafkaBroker, String groupId) {
        this.consumer = KafkaUtils.createKafkaConsumer(kafkaBroker, groupId);
        this.producer = KafkaUtils.createKafkaProducer(kafkaBroker);
    }

    public void sendMessage(String message) {
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(KafkaUtils.KAFKA_TOPIC_IN, message);
            producer.send(record);
            LogUtils.log("Sending message");
        } finally {
            producer.flush();
            producer.close();
        }

    }

    public String receiveAnswer(String rqUID) {
        Instant start = Instant.now();
        while(true) {
            Instant end = Instant.now();
            if (Duration.between(start, end).getSeconds() > HALF_HOUR_TIMEOUT) {
                throw new RuntimeException("Ошибка превышения таймаута ожидания ответа!");
            }
            ConsumerRecords<String, String> records = consumer.poll(1000L);
            if (records.count() != 0) {
                AtomicReference<String> answer = new AtomicReference<>();
                records.forEach(record -> {
                    String value = record.value();
                    if (value != null && value.contains(rqUID)) {
                        answer.set(value);
                    }
                });
                if (answer.get() != null) {
                    consumer.close();
                    return answer.get();
                }
            }
        }
    }
}
