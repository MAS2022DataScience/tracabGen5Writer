package com.mas2022datascience.tracabgen5writer.producer;

import com.mas2022datascience.avro.v1.GeneralMatch;
import com.mas2022datascience.avro.v1.GeneralMatchPhase;
import com.mas2022datascience.avro.v1.GeneralMatchPlayer;
import com.mas2022datascience.avro.v1.GeneralMatchTeam;
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
  private KafkaTemplate<String, TracabGen5TF01> kafkaTemplateTracabGen5;

  @Autowired
  private KafkaTemplate<String, GeneralMatch> kafkaTemplateTracabGen5Match;

  @Autowired
  private KafkaTemplate<String, GeneralMatchPhase> kafkaTemplateTracabGen5MatchPhase;

  @Autowired
  private KafkaTemplate<String, GeneralMatchPlayer> kafkaTemplateTracabGen5MatchPlayer;

  @Autowired
  private KafkaTemplate<String, GeneralMatchTeam> kafkaTemplateTracabGen5MatchTeam;

  public void produceTracabGen5(String topic, String key, TracabGen5TF01 value) {

    SendResult<String, TracabGen5TF01> result = null;
    try {
      result = kafkaTemplateTracabGen5.send(topic, key, value).get(10, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e ) {
      e.printStackTrace();
    }

    System.out.printf("sent TracabGen5TF01 record(key=%s, offset=%d)\n",key,result.getRecordMetadata().offset());

  }

  public void produceTracabGen5Match(String topic, String key, GeneralMatch value) {

    SendResult<String, GeneralMatch> result = null;
    try {
      result = kafkaTemplateTracabGen5Match.send(topic, key, value).get(10, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e ) {
      e.printStackTrace();
    }

    System.out.printf("sent TracabGen5Match record(key=%s, offset=%d)\n",key,result.getRecordMetadata().offset());

  }

  public void produceTracabGen5MatchPhase(String topic, String key, GeneralMatchPhase value) {

    SendResult<String, GeneralMatchPhase> result = null;
    try {
      result = kafkaTemplateTracabGen5MatchPhase.send(topic, key, value).get(10, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e ) {
      e.printStackTrace();
    }

    System.out.printf("sent TracabGen5MatchPhase record(key=%s, offset=%d)\n",key,result.getRecordMetadata().offset());

  }

  public void produceTracabGen5MatchTeam(String topic, String key, GeneralMatchTeam value) {

    SendResult<String, GeneralMatchTeam> result = null;
    try {
      result = kafkaTemplateTracabGen5MatchTeam.send(topic, key, value).get(10, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e ) {
      e.printStackTrace();
    }

    System.out.printf("sent TracabGen5MatchTeam record(key=%s, offset=%d)\n",key,result.getRecordMetadata().offset());

  }

  public void produceTracabGen5MatchPlayer(String topic, String key, GeneralMatchPlayer value) {

    SendResult<String, GeneralMatchPlayer> result = null;
    try {
      result = kafkaTemplateTracabGen5MatchPlayer.send(topic, key, value).get(10, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e ) {
      e.printStackTrace();
    }

    System.out.printf("sent TracabGen5MatchPlayer record(key=%s, offset=%d)\n",key,result.getRecordMetadata().offset());

  }
}
