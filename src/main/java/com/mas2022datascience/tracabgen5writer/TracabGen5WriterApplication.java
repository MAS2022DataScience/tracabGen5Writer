package com.mas2022datascience.tracabgen5writer;

import static com.mas2022datascience.util.Time.utcString2epocMs;

import com.google.gson.Gson;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;
import com.mas2022datascience.avro.v1.GeneralMatch;
import com.mas2022datascience.avro.v1.GeneralMatchPhase;
import com.mas2022datascience.avro.v1.GeneralMatchPlayer;
import com.mas2022datascience.avro.v1.GeneralMatchTeam;
import com.mas2022datascience.avro.v1.Object;
import com.mas2022datascience.avro.v1.Phase;
import com.mas2022datascience.avro.v1.TracabGen5TF01;
import com.mas2022datascience.avro.v1.TracabGen5TF01Metadata;
import com.mas2022datascience.avro.v1.TracabGen5TF01Player;
import com.mas2022datascience.tracabgen5writer.producer.KafkaTracabProducer;
import com.mas2022datascience.util.Player;
import com.mas2022datascience.util.Team;
import com.mas2022datascience.util.Time;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class TracabGen5WriterApplication implements CommandLineRunner {

	final private static Logger LOG = LoggerFactory.getLogger(TracabGen5WriterApplication.class);

	private final KafkaTracabProducer kafkaTracabProducer;

	// Initial time of the match
	@Value(value = "${writer.tracab.gen5.time-start}")
	private String initialTime;

	// Filepaths
	@Value(value = "${file.raw.filepath}")
	private String rawFilePath;

	@Value(value = "${file.metadata.filepath}")
	private String metadataFilePath;

	// Topics
	@Value(value = "${topic.tracab.name}")
	private String tracabGen5RawTopic;

	@Value(value = "${topic.general-match.name}")
	private String tracabGeneralMatchTopic;

	@Value(value = "${topic.general-match-phase.name}")
	private String tracabGeneralMatchPhaseTopic;

	@Value(value = "${topic.general-match-team.name}")
	private String tracabGeneralMatchTeamTopic;

	@Value(value = "${topic.general-match-player.name}")
	private String tracabGeneralMatchPlayerTopic;

	public TracabGen5WriterApplication(KafkaTracabProducer kafkaTracabProducer) {
		this.kafkaTracabProducer = kafkaTracabProducer;
	}

	public static void main(String[] args) {
		ConfigurableApplicationContext ctx = SpringApplication.run(TracabGen5WriterApplication.class,
				args);
		SpringApplication.exit(ctx, () -> 0);
	}

	@Override
	public void run(String... args) {
		LOG.info("");
		if (args.length == 0) {
			runProducer();
		}
	}

	private void runProducer() {
		LOG.info("Running producer");
		TracabGen5TF01Metadata metadata = null;

		long initialFrameNumber = 0;

		// Read initial offset number
		try {
			// Open the file
			BufferedReader br = new BufferedReader(new FileReader(rawFilePath));

			// Read the file line by line
			initialFrameNumber = Long.parseLong(br.readLine().split(":")[0]);

			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		// Read metadata
		try {
			// Create a Gson instance
			Gson gson = new Gson();

			// Read the JSON file
			metadata = gson.fromJson(new FileReader(metadataFilePath), TracabGen5TF01Metadata.class);

			// GeneralMatch information
			kafkaTracabProducer.produceTracabGen5Match(tracabGeneralMatchTopic,
					Integer.toString(metadata.getGameID()),
					GeneralMatch
							.newBuilder()
							.setPitchShortSide(metadata.getPitchShortSide())
							.setPitchLongSide(metadata.getPitchLongSide())
							.build()
			);

			// GeneralMatchTeam information
			kafkaTracabProducer.produceTracabGen5MatchTeam(tracabGeneralMatchTeamTopic,
					Integer.toString(metadata.getGameID()),
					GeneralMatchTeam
							.newBuilder()
							.setHomeTeamID(metadata.getHomeTeam().getTeamID())
							.setHomeShortName(metadata.getHomeTeam().getShortName())
							.setHomeLongName(metadata.getHomeTeam().getLongName())
							.setAwayTeamID(metadata.getAwayTeam().getTeamID())
							.setAwayShortName(metadata.getAwayTeam().getShortName())
							.setAwayLongName(metadata.getAwayTeam().getLongName())
							.build()
			);

			// GeneralMatchPlayer information
			// Home team
			List<TracabGen5TF01Player> players = new ArrayList<>();
			for (TracabGen5TF01Player player : metadata.getHomeTeam().getPlayers()) {
				players.add(TracabGen5TF01Player
						.newBuilder()
						.setTeamID(metadata.getHomeTeam().getTeamID())
						.setPlayerID(player.getPlayerID())
						.setFirstName(player.getFirstName())
						.setLastName(player.getLastName())
						.setJerseyNo(player.getJerseyNo())
						.setStartFrameCount(player.getStartFrameCount())
						.setStartTime(
								Time.getUTCStringFromOffsetValue(player.getStartFrameCount(), metadata.getFrameRate(),
										initialFrameNumber, initialTime))
						.setEndFrameCount(player.getEndFrameCount())
						.setEndTime(
								Time.getUTCStringFromOffsetValue(player.getEndFrameCount(), metadata.getFrameRate(),
										initialFrameNumber, initialTime))
						.build()
				);
			}
			// Away team
			for (TracabGen5TF01Player player : metadata.getAwayTeam().getPlayers()) {
				players.add(TracabGen5TF01Player
						.newBuilder()
						.setTeamID(metadata.getAwayTeam().getTeamID())
						.setPlayerID(player.getPlayerID())
						.setFirstName(player.getFirstName())
						.setLastName(player.getLastName())
						.setJerseyNo(player.getJerseyNo())
						.setStartFrameCount(player.getStartFrameCount())
						.setStartTime(
								Time.getUTCStringFromOffsetValue(player.getStartFrameCount(), metadata.getFrameRate(),
										initialFrameNumber, initialTime))
						.setEndFrameCount(player.getEndFrameCount())
						.setEndTime(
								Time.getUTCStringFromOffsetValue(player.getEndFrameCount(), metadata.getFrameRate(),
										initialFrameNumber, initialTime))
						.build()
				);
			}
			kafkaTracabProducer.produceTracabGen5MatchPlayer(tracabGeneralMatchPlayerTopic,
					Integer.toString(metadata.getGameID()),
					GeneralMatchPlayer
							.newBuilder()
							.setPlayers(players)
							.build()
			);

		} catch (JsonIOException | JsonSyntaxException | IOException e) {
			e.printStackTrace();
		}

		List<Phase> phases = new ArrayList<>();
		// Read RAW data
		try {
			// Open the file
			BufferedReader br = new BufferedReader(new FileReader(rawFilePath));

			// Read the file line by line
			String line;
			line = br.readLine();
			processData(line, metadata, initialFrameNumber, phases);

			while ((line = br.readLine()) != null) {
				processData(line, metadata, initialFrameNumber, phases);
			}

			// Close the file
			br.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void processData(String line, TracabGen5TF01Metadata metadata, long initialFrameNumber,
			List<Phase> phases) {

		String[] lineSplit = line.split(":");
		// chunk 1 is the offset counter

		// chunk 2 contains the player and referee data
		// Data type: String represented array of up to 29 objects
		// Each object contains the following properties:
		// (0) targets assigned team*, (1) system target ID, (2) assigned jersey** number,
		// (3) pitch position x***, (4) pitch position y***, (5) target speed****
		ArrayList<Object> objects = new ArrayList<>();
		String[] chunk2 = lineSplit[1].split(";");
		for (int i = 0; i < chunk2.length-1; i++) {
			String[] objectData = chunk2[i].split(",");
			if (!objectData[0].equals("-1")) {
				objects.add(Object.newBuilder()
						.setType(Integer.parseInt(objectData[0]))
						.setId(objectData[2])
						.setPlayerId(Player.getPlayerIdJerseyNumber(objectData[2], objectData[0], metadata))
						.setTeamId(Team.getTeamIdFromHomeOrAwayId(Integer.parseInt(objectData[0]), metadata))
						.setX(Integer.parseInt(objectData[3]))
						.setY(Integer.parseInt(objectData[4]))
						.setZ(0)
						.setSampling(0)
						.setVelocity(Double.parseDouble(objectData[5]))
						.build());
			}
		}

		// chunk 3 contains the ball data and the optional data
		// Object contains the following properties: (0) pitch position x*, (1) pitch position y*,
		// (2) pitch position z*, (3) ball speed**,
		// (4) ball owning team*** -> "H" (home) or "A" (away).
		// (5) ball status**** -> "Alive" or "Dead".
		// (6) (not always set) ball contact device info 1 ***** -> ignored
		// (7) (not always set) ball contact device info 2 ***** -> ignored
		String[] chunk3 = lineSplit[2].replace(";","").split(",");

		// phases
		switch (phases.size()) {
			case 0 -> {
				if (Integer.parseInt(lineSplit[0])>=metadata.getPhase1StartFrame()
						&& chunk3[5].equals("Alive") && metadata.getPhase1StartFrame() != 0) {
					phases.add(Phase.newBuilder()
							.setPhaseNumber(1)
							.setStart(
									Time.getUTCStringFromOffsetValue(metadata.getPhase1StartFrame(), metadata.getFrameRate(),
										initialFrameNumber, initialTime))
							.setEnd(
									Time.getUTCStringFromOffsetValue(metadata.getPhase1EndFrame(), metadata.getFrameRate(),
										initialFrameNumber, initialTime))
							.setLeftTeamID(Team.getLeftTeamID(chunk2, metadata))
						.build()
					);
					// Produce the metadata phases
					kafkaTracabProducer.produceTracabGen5MatchPhase(tracabGeneralMatchPhaseTopic,
							Integer.toString(metadata.getGameID()),
							GeneralMatchPhase.newBuilder()
									.setPhases(phases)
									.build()
					);
				}
			}
			case 1 -> {
				if (Integer.parseInt(lineSplit[0])>=metadata.getPhase2StartFrame()
						&& chunk3[5].equals("Alive") && metadata.getPhase2StartFrame() != 0) {
					phases.add(Phase.newBuilder()
							.setPhaseNumber(2)
							.setStart(
									Time.getUTCStringFromOffsetValue(metadata.getPhase1StartFrame(), metadata.getFrameRate(),
											initialFrameNumber, initialTime))
							.setEnd(
									Time.getUTCStringFromOffsetValue(metadata.getPhase1EndFrame(), metadata.getFrameRate(),
											initialFrameNumber, initialTime))
							.setLeftTeamID(Team.getLeftTeamID(chunk2, metadata))
							.build()
					);
					// Produce the metadata phases
					kafkaTracabProducer.produceTracabGen5MatchPhase(tracabGeneralMatchPhaseTopic,
							Integer.toString(metadata.getGameID()),
							GeneralMatchPhase.newBuilder()
									.setPhases(phases)
									.build()
					);
				}
			}
			case 2 -> {
				if (Integer.parseInt(lineSplit[0])>=metadata.getPhase3StartFrame()
						&& chunk3[5].equals("Alive") && metadata.getPhase3StartFrame() != 0) {
					phases.add(Phase.newBuilder()
							.setPhaseNumber(3)
							.setStart(
									Time.getUTCStringFromOffsetValue(metadata.getPhase1StartFrame(), metadata.getFrameRate(),
											initialFrameNumber, initialTime))
							.setEnd(
									Time.getUTCStringFromOffsetValue(metadata.getPhase1EndFrame(), metadata.getFrameRate(),
											initialFrameNumber, initialTime))
							.setLeftTeamID(Team.getLeftTeamID(chunk2, metadata))
							.build()
					);
					// Produce the metadata phases
					kafkaTracabProducer.produceTracabGen5MatchPhase(tracabGeneralMatchPhaseTopic,
							Integer.toString(metadata.getGameID()),
							GeneralMatchPhase.newBuilder()
									.setPhases(phases)
									.build()
					);
				}
			}
			case 3 -> {
				if (Integer.parseInt(lineSplit[0])>=metadata.getPhase4StartFrame()
						&& chunk3[5].equals("Alive") && metadata.getPhase4StartFrame() != 0) {
					phases.add(Phase.newBuilder()
							.setPhaseNumber(4)
							.setStart(
									Time.getUTCStringFromOffsetValue(metadata.getPhase1StartFrame(), metadata.getFrameRate(),
											initialFrameNumber, initialTime))
							.setEnd(
									Time.getUTCStringFromOffsetValue(metadata.getPhase1EndFrame(), metadata.getFrameRate(),
											initialFrameNumber, initialTime))
							.setLeftTeamID(Team.getLeftTeamID(chunk2, metadata))
							.build()
					);
					// Produce the metadata phases
					kafkaTracabProducer.produceTracabGen5MatchPhase(tracabGeneralMatchPhaseTopic,
							Integer.toString(metadata.getGameID()),
							GeneralMatchPhase.newBuilder()
									.setPhases(phases)
									.build()
					);
				}
			}
			case 4 -> {
				if (Integer.parseInt(lineSplit[0])>=metadata.getPhase5StartFrame()
						&& chunk3[5].equals("Alive") && metadata.getPhase5StartFrame() != 0) {
					phases.add(Phase.newBuilder()
							.setPhaseNumber(5)
							.setStart(
									Time.getUTCStringFromOffsetValue(metadata.getPhase1StartFrame(), metadata.getFrameRate(),
											initialFrameNumber, initialTime))
							.setEnd(
									Time.getUTCStringFromOffsetValue(metadata.getPhase1EndFrame(), metadata.getFrameRate(),
											initialFrameNumber, initialTime))
							.setLeftTeamID(Team.getLeftTeamID(chunk2, metadata))
							.build()
					);
					// Produce the metadata phases
					kafkaTracabProducer.produceTracabGen5MatchPhase(tracabGeneralMatchPhaseTopic,
							Integer.toString(metadata.getGameID()),
							GeneralMatchPhase.newBuilder()
									.setPhases(phases)
									.build()
					);
				}
			}
			default -> {}
		}

		// ball
		objects.add(Object.newBuilder()
				.setType(7)
				.setId("0")
				.setX(Integer.parseInt(chunk3[0]))
				.setY(Integer.parseInt(chunk3[1]))
				.setZ(Integer.parseInt(chunk3[2]))
				.setSampling(0)
				.setVelocity(Double.parseDouble(chunk3[3]))
				.build());

		String ballOwningTeam = "";
		String isBallInPlay = "";
		String ballContactDevice1 = "";

		switch (chunk3.length) {
			case 5 -> ballOwningTeam = chunk3[4];
			case 6 -> {
				ballOwningTeam = chunk3[4];
				isBallInPlay = chunk3[5];
			}
			case 7 -> {
				ballOwningTeam = chunk3[4];
				isBallInPlay = chunk3[5];
				ballContactDevice1 = chunk3[6];
			}
			default -> {
			}
		}

		kafkaTracabProducer.produceTracabGen5(tracabGen5RawTopic, String.valueOf(metadata.getGameID()),
				TracabGen5TF01
					.newBuilder()
						.setTs(
								Instant.ofEpochMilli(utcString2epocMs(
										Time.getUTCStringFromOffsetValue(Long.parseLong(lineSplit[0]),
												metadata.getFrameRate(),
												initialFrameNumber, initialTime))))
						.setUtc(
								Time.getUTCStringFromOffsetValue(Long.parseLong(lineSplit[0]), metadata.getFrameRate(),
										initialFrameNumber, initialTime))
						.setMatchId(String.valueOf(metadata.getGameID()))
						.setBallPossession(ballOwningTeam)
						.setIsBallInPlay(isBallInPlay)
						.setContactDevInfo(ballContactDevice1)
						.setObjects(objects)
						.build());

	}

}


