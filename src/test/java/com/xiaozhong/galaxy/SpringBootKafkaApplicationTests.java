package com.xiaozhong.galaxy;

import org.apache.kafka.clients.producer.*;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@RunWith(SpringRunner.class)
@SpringBootTest
public class SpringBootKafkaApplicationTests {

	@Test
	public void testKafkaAsyncSendMessage() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("ack", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        //缓存每个分区未发送的消息
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(properties);
        for (int i = 0; i < 100; i ++) {
            producer.send(new ProducerRecord<>("test", Integer.toString(i), Integer.toString(i)));
        }
        producer.close();
    }

    @Test
    public void testKafkaSyncSendMessage() throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("ack", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        //缓存每个分区未发送的消息
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        Producer<byte[], byte[]> producer = new KafkaProducer<byte[], byte[]>(properties);

        byte[] key = "key".getBytes();
        byte[] value = "value".getBytes();
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>("test", key, value);

        Future<RecordMetadata> result =  producer.send(record);
        //阻塞调用
        System.out.println(result.get());
        //print: test-0@104
    }

    @Test
    public void testKafkaSendMessageCallBack() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("ack", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        //缓存每个分区未发送的消息
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        Producer<byte[], byte[]> producer = new KafkaProducer<>(properties);

        Callback callback1 = (recordMetadata, e) -> System.out.println("The offset of the record we just sent is : " + recordMetadata.offset());
        Callback callback2 = (recordMetadata, e) -> System.out.println("The offset of the record we just sent is : " + recordMetadata.offset());

        producer.send(new ProducerRecord<>("test", 0, "first".getBytes(), "first".getBytes()), callback1);
        producer.send(new ProducerRecord<>("test", 0, "second".getBytes(), "second".getBytes()), callback2);
        //print: first second
    }

}
