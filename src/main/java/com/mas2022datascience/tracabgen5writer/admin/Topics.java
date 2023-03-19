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
        .config(TopicConfig.DELETE_RETENTION_MS_CONFIG, "10")
        .config(TopicConfig.SEGMENT_MS_CONFIG, "100")
        .config(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.01")
        .config(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, "0")
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
        .config(TopicConfig.DELETE_RETENTION_MS_CONFIG, "10")
        .config(TopicConfig.SEGMENT_MS_CONFIG, "100")
        .config(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.01")
        .config(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, "0")
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
        .config(TopicConfig.DELETE_RETENTION_MS_CONFIG, "10")
        .config(TopicConfig.SEGMENT_MS_CONFIG, "100")
        .config(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.01")
        .config(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, "0")
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
        .config(TopicConfig.DELETE_RETENTION_MS_CONFIG, "10")
        .config(TopicConfig.SEGMENT_MS_CONFIG, "100")
        .config(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.01")
        .config(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, "0")
        .build();
  }

  @Value(value = "${topic.tracab-02.name}")
  private String topicName2;
  @Value(value = "${topic.tracab-02.partitions}")
  private Integer topicPartitions2;
  @Value(value = "${topic.tracab-02.replication-factor}")
  private Integer topicReplicationFactor2;

  // creates or alters the topic
  @Bean
  public NewTopic tracab02() {
    return TopicBuilder.name(topicName2)
        .partitions(topicPartitions2)
        .replicas(topicReplicationFactor2)
        .config(TopicConfig.RETENTION_MS_CONFIG, "-1")
        .build();
  }

  @Value(value = "${topic.tracab-03.name}")
  private String topicName3;
  @Value(value = "${topic.tracab-03.partitions}")
  private Integer topicPartitions3;
  @Value(value = "${topic.tracab-03.replication-factor}")
  private Integer topicReplicationFactor3;

  // creates or alters the topic
  @Bean
  public NewTopic tracab03() {
    return TopicBuilder.name(topicName3)
        .partitions(topicPartitions3)
        .replicas(topicReplicationFactor3)
        .config(TopicConfig.RETENTION_MS_CONFIG, "-1")
        .build();
  }

  @Value(value = "${topic.general-01.name}")
  private String topicNamePlayerBall;
  @Value(value = "${topic.general-01.partitions}")
  private Integer topicPartitionsPlayerBall;
  @Value(value = "${topic.general-01.replication-factor}")
  private Integer topicReplicationFactorPlayerBall;

  // creates or alters the topic
  @Bean
  public NewTopic general01() {
    return TopicBuilder.name(topicNamePlayerBall)
        .partitions(topicPartitionsPlayerBall)
        .replicas(topicReplicationFactorPlayerBall)
        .config(TopicConfig.RETENTION_MS_CONFIG, "-1")
        .build();
  }

  @Value(value = "${topic.general-02.name}")
  private String topicNameTeam;
  @Value(value = "${topic.general-02.partitions}")
  private Integer topicPartitionsTeam;
  @Value(value = "${topic.general-02.replication-factor}")
  private Integer topicReplicationFactorTeam;

  // creates or alters the topic
  @Bean
  public NewTopic general02() {
    return TopicBuilder.name(topicNameTeam)
        .partitions(topicPartitionsTeam)
        .replicas(topicReplicationFactorTeam)
        .config(TopicConfig.RETENTION_MS_CONFIG, "-1")
        .build();
  }

  @Value(value = "${topic.general-03.name}")
  private String topicNamePlayerBallCompact;
  @Value(value = "${topic.general-03.partitions}")
  private Integer topicPartitionsPlayerBallCompact;
  @Value(value = "${topic.general-03.replication-factor}")
  private Integer topicReplicationFactorPlayerBallCompact;

  // creates or alters the topic
  @Bean
  public NewTopic general03() {
    return TopicBuilder.name(topicNamePlayerBallCompact)
        .partitions(topicPartitionsPlayerBallCompact)
        .replicas(topicReplicationFactorPlayerBallCompact)
        .config(TopicConfig.RETENTION_MS_CONFIG, "-1")
        .build();
  }

  @Value(value = "${topic.general-acceleration.name}")
  private String topicNameGeneral03;
  @Value(value = "${topic.general-acceleration.partitions}")
  private Integer topicPartitionsGeneral03;
  @Value(value = "${topic.general-acceleration.replication-factor}")
  private Integer topicReplicationFactorGeneral03;

  // creates the topic
  @Bean
  public NewTopic generalAcc03() {
    return TopicBuilder.name(topicNameGeneral03)
        .partitions(topicPartitionsGeneral03)
        .replicas(topicReplicationFactorGeneral03)
        .config(TopicConfig.RETENTION_MS_CONFIG, "-1")
        .build();
  }

  @Value(value = "${topic.general-ball-zone-change.name}")
  private String topicNameBallZoneChange;
  @Value(value = "${topic.general-ball-zone-change.partitions}")
  private Integer topicPartitionsBallZoneChange;
  @Value(value = "${topic.general-ball-zone-change.replication-factor}")
  private Integer topicReplicationFactorBallZoneChange;

  // creates or alters the topic
  @Bean
  public NewTopic generalBallZoneChange() {
    return TopicBuilder.name(topicNameBallZoneChange)
        .partitions(topicPartitionsBallZoneChange)
        .replicas(topicReplicationFactorBallZoneChange)
        .config(TopicConfig.RETENTION_MS_CONFIG, "-1")
        .build();
  }

  @Value(value = "${topic.general-ball-possession-change.name}")
  private String topicNameBallPossessionChange;
  @Value(value = "${topic.general-ball-possession-change.partitions}")
  private Integer topicPartitionsBallPossessionChange;
  @Value(value = "${topic.general-ball-possession-change.replication-factor}")
  private Integer topicReplicationFactorBallPossessionChange;

  // creates or alters the topic
  @Bean
  public NewTopic generalBallPossessionChange() {
    return TopicBuilder.name(topicNameBallPossessionChange)
        .partitions(topicPartitionsBallPossessionChange)
        .replicas(topicReplicationFactorBallPossessionChange)
        .config(TopicConfig.RETENTION_MS_CONFIG, "-1")
        .build();
  }

}
