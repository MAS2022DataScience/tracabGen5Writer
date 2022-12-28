package com.mas2022datascience.tracabgen5writer.producer;

import com.mas2022datascience.avro.v1.TracabGen5TF01;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

@Component
public class KafkaTracabProducer {
  @Autowired
  private KafkaTemplate<String, TracabGen5TF01> kafkaTemplate;

  public void produce(String topic, String key, TracabGen5TF01 value) {

    SendResult<String, TracabGen5TF01> result = null;
    try {
      result = kafkaTemplate.send(topic, key, value).get(10, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e ) {
      e.printStackTrace();
    }

    System.out.printf("sent record(key=%s, offset=%d)\n",key,result.getRecordMetadata().offset());

  }

}
