package org.wong.kafka.test.producer;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 *  custom kafka interceptor
 *  1. extends ProducerInterceptor interface
 *  2. define generic type
 *  3. override methods
 */
public class ValueInterceptor implements ProducerInterceptor<String, String> {

    // when send data, will invoke this method
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return new ProducerRecord<String, String>(record.topic(), record.key(), record.value()+record.value());
    }

    // after sent data, when server send back response, will invoke this method
    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    // producer object when closed will invoke this method
    @Override
    public void close() {

    }


    // during creation of producer object will invoke
    @Override
    public void configure(Map<String, ?> configs) {

    }
}
