package com.mas2022datascience.tracabgen5writer;

import com.mas2022datascience.avro.v1.Competition;
import com.mas2022datascience.avro.v1.Frame;
import com.mas2022datascience.avro.v1.Match;
import com.mas2022datascience.avro.v1.Object;
import com.mas2022datascience.avro.v1.Phase;
import com.mas2022datascience.avro.v1.Stadium;
import com.mas2022datascience.tracabsimulator.producer.KafkaTracabProducer;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

@SpringBootApplication
public class TracabSimulatorApplication implements CommandLineRunner {

	final private static Logger LOG = LoggerFactory.getLogger(TracabSimulatorApplication.class);

	private final KafkaTracabProducer kafkaTracabProducer;

	public TracabSimulatorApplication(KafkaTracabProducer kafkaTracabProducer) {
		this.kafkaTracabProducer = kafkaTracabProducer;
	}

	public static void main(String[] args) {
		ConfigurableApplicationContext ctx = SpringApplication.run(TracabSimulatorApplication.class, args);
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
		try {
			//creating a constructor of file class and parsing an XML file
			File file = new File("./tracab/POR - SUI - Tracking Raw Data - xml - xml.xml");
			//an instance of factory that gives a document builder
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			//an instance of builder to parse the specified xml file
			DocumentBuilder db = dbf.newDocumentBuilder();
			Document doc = db.parse(file);
			doc.getDocumentElement().normalize();

			//Match Information
			NodeList matchNodeList = doc.getElementsByTagName("Match");
			Node matchNode = matchNodeList.item(0);
			Element matchElem = (Element) matchNode;
			String matchId = matchElem.getAttribute("id");
			Match match = Match.newBuilder()
					.setId(Integer.parseInt(matchId))
					.setDateMatch(matchElem.getAttribute("dateMatch"))
					.setMatchNumber(Integer.parseInt(matchElem.getAttribute("matchNumber")))
					.build();
			//<Match id="2024419" dateMatch="2019-06-05T18:45:00" matchNumber="139">

			//Competition Information
			NodeList competitionNodeList = doc.getElementsByTagName("Competition");
			Node competitionNode = competitionNodeList.item(0);
			Element competitionElem = (Element) competitionNode;
			Competition competition = Competition.newBuilder()
					.setId(Integer.parseInt(competitionElem.getAttribute("id")))
					.setName(competitionElem.getAttribute("name"))
					.build();
			//<Competition id="20192" name="UEFA Nations League 2019" />

			//Stadium Information
			NodeList stadiumNodeList = doc.getElementsByTagName("Stadium");
			Node stadiumNode = stadiumNodeList.item(0);
			Element stadiumElem = (Element) stadiumNode;
			Stadium stadium = Stadium.newBuilder()
					.setId(Integer.parseInt(stadiumElem.getAttribute("id")))
					.setName(stadiumElem.getAttribute("name"))
					.setPitchLength(Integer.parseInt(stadiumElem.getAttribute("pitchLength")))
					.setPitchWidth(Integer.parseInt(stadiumElem.getAttribute("pitchWidth")))
					.build();
			//<Stadium id="85429" name="Estádio do Dragão" pitchLength="10500" pitchWidth="6800" />

			//Phases Information
			List<Phase> phases = new ArrayList<>();
			NodeList phaseNodeList = doc.getElementsByTagName("Phase");
			Node phaseNode = phaseNodeList.item(0);
			Element phaseElem = (Element) phaseNode;
			phases.add(
					Phase.newBuilder()
							.setStart(fixUtcString(phaseElem.getAttribute("start")))
							.setEnd(fixUtcString(phaseElem.getAttribute("end")))
							.setLeftTeamID(Integer.parseInt(phaseElem.getAttribute("leftTeamID")))
							.build()
			);
			//<Phase start="2019-06-05T18:46:49.43" end="2019-06-05T19:32:53.14" leftTeamID="110" />
			phaseNode = phaseNodeList.item(1);
			phaseElem = (Element) phaseNode;
			phases.add(
					Phase.newBuilder()
					.setStart(fixUtcString(phaseElem.getAttribute("start")))
					.setEnd(fixUtcString(phaseElem.getAttribute("end")))
					.setLeftTeamID(Integer.parseInt(phaseElem.getAttribute("leftTeamID")))
					.build()
			);
			//<Phase start="2019-06-05T19:47:59.794" end="2019-06-05T20:38:38.079" leftTeamID="128" />

			// Ball and Player Information
//			<Frame utc="2019-06-05T18:47:25.843" isBallInPlay="1" ballPossession="Away">
//        <Objs>
//          <Obj type="7" id="0" x="4111" y="2942" z="11" sampling="0" />
//          <Obj type="0" id="63706" x="3684" y="3059" sampling="0" />
//			    ...
//          <Obj type="1" id="1905360" x="694" y="2007" sampling="0" />
//        </Objs>
//      </Frame>
			NodeList frameNodeList = doc.getElementsByTagName("Frame");
			for (int itr = 0; itr < frameNodeList.getLength(); itr++) {
				Node frameNode = frameNodeList.item(itr);
				if (frameNode.getNodeType() == Node.ELEMENT_NODE) {
					Element frameElem = (Element) frameNode;

					String isBallInPlayString = frameElem.getAttribute("isBallInPlay");

					String utcString = frameElem.getAttribute("utc");
					utcString = fixUtcString(utcString);
					String ballPossession = frameElem.getAttribute("ballPossession");
//					<Frame utc="2019-06-05T18:47:25.843" isBallInPlay="1" ballPossession="Away">

					List<Object> objects = new ArrayList<>();
					NodeList objNodeList = frameElem.getElementsByTagName("Obj");
					for (int i = 0; i < objNodeList.getLength(); i++) {
						Node objNode = objNodeList.item(i);
						if (objNode.getNodeType() == Node.ELEMENT_NODE) {
							Element objElem = (Element) objNode;
							String objId = objElem.getAttribute("id");

							if (!objId.equals("0")) {
								objects.add(
										Object.newBuilder()
												.setId(objElem.getAttribute("id"))
												.setType(Integer.parseInt(objElem.getAttribute("type")))
												.setX(Integer.parseInt(objElem.getAttribute("x")))
												.setY(Integer.parseInt(objElem.getAttribute("y")))
												.setZ(0)
												.setSampling(Integer.parseInt(objElem.getAttribute("sampling")))
												.build()
								);
							} else {
								//ball
								objects.add(
										Object.newBuilder()
												.setId(objElem.getAttribute("id"))
												.setType(Integer.parseInt(objElem.getAttribute("type")))
												.setX(Integer.parseInt(objElem.getAttribute("x")))
												.setY(Integer.parseInt(objElem.getAttribute("y")))
												.setZ(Integer.parseInt(objElem.getAttribute("z")))
												.setSampling(Integer.parseInt(objElem.getAttribute("sampling")))
												.build()
								);
							}
						}
					}
					kafkaTracabProducer.produce(matchId, Frame.newBuilder()
									.setUtc(utcString)
									.setBallPossession(ballPossession)
									.setIsBallInPlay(Integer.parseInt(isBallInPlayString))
									.setObjects(objects)
									.setMatch(match)
									.setStadium(stadium)
									.setPhases(phases)
									.setCompetition(competition)
									.build()
					);
				}
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	/**
	 * if the UTC string is not correctly formatted (yyyy-MM-dd'T'HH:mm:ss.SSS) it gets corrected
	 * @param utcString of type String of format 'yyyy-MM-dd'T'HH:mm:ss.SSS'
	 * @return fixed utcString of format 'yyyy-MM-dd'T'HH:mm:ss.SSS'
	 */
	private static String fixUtcString(String utcString) {
		// fix utc time format
		if (utcString.length() == 19) {
			utcString = utcString + ".000";
		}
		utcString = (utcString + "000").substring(0, 23);
		return utcString;
	}
}
