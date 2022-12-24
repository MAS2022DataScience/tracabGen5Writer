package com.mas2022datascience.tracabgen5writer.admin;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.stereotype.Component;

@Component
public class Topics {

  @Value(value = "${topic.tracab.name}")
  private String tracabTopic;
  @Value(value = "${topic.tracab.partitions}")
  private Integer tracabTopicPartitions;
  @Value(value = "${topic.tracab.replication-factor}")
  private Integer tracabTopicReplicationFactor;

  // creates or alters the topic
  @Bean
  public NewTopic tracab() {
    return TopicBuilder.name(tracabTopic)
        .partitions(tracabTopicPartitions)
        .replicas(tracabTopicReplicationFactor)
        .config(TopicConfig.RETENTION_MS_CONFIG, "-1")
        .build();
  }
}
