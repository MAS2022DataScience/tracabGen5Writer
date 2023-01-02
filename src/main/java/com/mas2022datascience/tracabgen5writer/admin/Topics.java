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
  private String tracabRawTopic;
  @Value(value = "${topic.tracab.partitions}")
  private Integer tracabRawPartitions;
  @Value(value = "${topic.tracab.replication-factor}")
  private Integer tracabRawReplicationFactor;

  // creates the topic if not existent
  @Bean
  public NewTopic tracabRaw() {
    return TopicBuilder.name(tracabRawTopic)
        .partitions(tracabRawPartitions)
        .replicas(tracabRawReplicationFactor)
        .config(TopicConfig.RETENTION_MS_CONFIG, "-1")
        .build();
  }

  @Value(value = "${topic.general-match.name}")
  private String generalMatchTopic;
  @Value(value = "${topic.tracab.partitions}")
  private Integer generalMatchPartitions;
  @Value(value = "${topic.tracab.replication-factor}")
  private Integer generalMatchReplicationFactor;

  // creates the topic if not existent
  @Bean
  public NewTopic generalMatch() {
    return TopicBuilder.name(generalMatchTopic)
        .partitions(generalMatchPartitions)
        .replicas(generalMatchReplicationFactor)
        .config(TopicConfig.RETENTION_MS_CONFIG, "-1")
        .config(TopicConfig.CLEANUP_POLICY_CONFIG, "compact")
        .build();
  }

  @Value(value = "${topic.general-match-phase.name}")
  private String generalMatchPhaseTopic;
  @Value(value = "${topic.tracab.partitions}")
  private Integer generalMatchPhasePartitions;
  @Value(value = "${topic.tracab.replication-factor}")
  private Integer generalMatchPhaseReplicationFactor;

  // creates the topic if not existent
  @Bean
  public NewTopic generalMatchPhase() {
    return TopicBuilder.name(generalMatchPhaseTopic)
        .partitions(generalMatchPhasePartitions)
        .replicas(generalMatchPhaseReplicationFactor)
        .config(TopicConfig.RETENTION_MS_CONFIG, "-1")
        .config(TopicConfig.CLEANUP_POLICY_CONFIG, "compact")
        .build();
  }
  @Value(value = "${topic.general-match-team.name}")
  private String generalMatchTeamTopic;
  @Value(value = "${topic.tracab.partitions}")
  private Integer generalMatchTeamPartitions;
  @Value(value = "${topic.tracab.replication-factor}")
  private Integer generalMatchTeamReplicationFactor;

  // creates the topic if not existent
  @Bean
  public NewTopic generalMatchTeam() {
    return TopicBuilder.name(generalMatchTeamTopic)
        .partitions(generalMatchTeamPartitions)
        .replicas(generalMatchTeamReplicationFactor)
        .config(TopicConfig.RETENTION_MS_CONFIG, "-1")
        .config(TopicConfig.CLEANUP_POLICY_CONFIG, "compact")
        .build();
  }
  @Value(value = "${topic.general-match-player.name}")
  private String generalMatchPlayerTopic;
  @Value(value = "${topic.tracab.partitions}")
  private Integer generalMatchPlayerPartitions;
  @Value(value = "${topic.tracab.replication-factor}")
  private Integer generalMatchPlayerReplicationFactor;

  // creates the topic if not existent
  @Bean
  public NewTopic generalMatchPlayer() {
    return TopicBuilder.name(generalMatchPlayerTopic)
        .partitions(generalMatchPlayerPartitions)
        .replicas(generalMatchPlayerReplicationFactor)
        .config(TopicConfig.RETENTION_MS_CONFIG, "-1")
        .config(TopicConfig.CLEANUP_POLICY_CONFIG, "compact")
        .build();
  }
}
