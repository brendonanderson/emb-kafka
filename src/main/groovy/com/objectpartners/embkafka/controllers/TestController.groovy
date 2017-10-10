package com.objectpartners.embkafka.controllers

import groovy.json.JsonSlurper
import groovy.util.logging.Slf4j
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestMethod
import org.springframework.web.bind.annotation.RestController

@RestController
@Slf4j
class TestController {

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate

    @RequestMapping(value = '/test', method = RequestMethod.POST)
    String test(@RequestBody String jsonString) {
        def json = new JsonSlurper().parseText(jsonString)
        kafkaTemplate.send('output-topic', json['key'] as String, jsonString).get()
        json
    }
}
