package com.objectpartners.embkafka.controllers

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import groovyx.net.http.ContentType
import groovyx.net.http.RESTClient
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import spock.lang.Specification

@CompileStatic
@Slf4j
class EmbKafkaFuncSpec extends Specification {

    static int port = System.getProperty('server.port') as int
    static String bootstrapServers = System.getProperty('spring.kafka.producer.bootstrap-servers')
    RESTClient restClient = new RESTClient("http://localhost:${port}")

    def postJson(String path, String json) {
        restClient.post(path: path, body: json, requestContentType: ContentType.JSON)
    }

    String consume(String topic, String key, int maxRetries, long pollMs) {
        int retry = 0
        String message = null
        KafkaConsumer<String, String> consumer = createKafkaConsumer(topic)

        while (!message && retry < maxRetries) {
            retry++
            ConsumerRecords consumerRecords = consumer.poll(pollMs)
            consumerRecords.each { ConsumerRecord record ->
                if (record.key() == key) {
                    message = record.value()
                }
            }
            consumer.commitSync()
        }
        consumer.close()
        return message
    }

    private static KafkaConsumer createKafkaConsumer(String topic) {
        Map props = new HashMap<>()
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer)
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer)
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, 'earliest')
        props.put(ConsumerConfig.GROUP_ID_CONFIG, 'func-test-group')
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 16777216)

        KafkaConsumer consumer = new KafkaConsumer(props)
        consumer.subscribe([topic])
        consumer
    }
}
