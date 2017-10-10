package com.objectpartners.embkafka.controllers

class TestControllerFuncSpec extends EmbKafkaFuncSpec {

    void 'testConsume'() {
        given:
        String key = '1'
        String json = """{"key":"${key}", "data": "foo"}"""

        when:
        postJson('/test', json)

        then:
        consume('output-topic', key, 5, 500)
    }

}
