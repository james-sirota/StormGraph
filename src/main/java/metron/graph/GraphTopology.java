/*
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package metron.graph;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpoutConfig.Builder;
import org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff;
import org.apache.storm.kafka.spout.KafkaSpoutRetryService;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;


public class GraphTopology {

	private static final Logger logger = LoggerFactory.getLogger(GraphTopology.class);

	public static void main(String[] args) throws Exception {

		if (args[0] == null)
			System.out.println("Please specify the location of the file graphtopology_config.conf");


		Config conf = readConfigFromFile(args[0], logger);

		TopologyBuilder builder = new TopologyBuilder();

		String spoutName = ConfigHandler.checkForNullConfigAndLoad("top.spout.name", conf);
		int spoutParallelism = Integer.parseInt(ConfigHandler.checkForNullConfigAndLoad("top.spout.parallelism", conf));
		boolean generateData = Boolean
				.parseBoolean(ConfigHandler.checkForNullConfigAndLoad("top.generatorSpoutEnabled", conf));

		if (generateData) {

			System.out.println("Started initializing generator spout");
			System.out.println(
					"Setting up generator spout with name " + spoutName + " and parallelism of " + spoutParallelism);
			builder.setSpout(spoutName, new TelemetryLoaderSpout(), spoutParallelism);
			System.out.println("Finished initializing the generator spout");

		} 
		
		else {
			
			String bootstrapServersParam = "top.spout.kafka.bootStrapServers";
			String topicParam = "top.spout.kafka.topic";
			String consumerGroupParam = "top.spout.kafka.consumerGroupId";
			String offsetCommitPeriodMsParam = "top.spout.kafka.offsetCommitPeriodMs";
			String initialDelayParam = "top.spout.kafka.retry.initialDelay";
			String delayPeriodParam = "top.spout.kafka.retry.delayPeriod";
			String maxDelayParam = "top.spout.kafka.retry.maxDelay";
			String uncommittedOffsetsParam = "top.spout.kafka.maxUncommittedOffsets";
			String tupleFieldTopicParam = "top.spout.kafka.tupleFieldTopic";
			String tupleFieldPartitionParam = "top.spout.kafka.tupleFieldPartition";
			String tupleFieldOffsetParam = "top.spout.kafka.tupleFieldOffset";
			String tupleFieldKeyParam = "top.spout.kafka.tupleFieldKey";
			String tupleFieldValueParam = "top.spout.kafka.tupleFieldValue";
			String forceFromStartParam = "top.spout.kafka.forceFromStart";

			System.out.println("[KAFKA_SPOUT] " + "Setting up kafka spout with the following arguments...");
			
			String bootStrapServers = ConfigHandler.checkForNullConfigAndLoad(bootstrapServersParam, conf);
			System.out.println("[KAFKA_SPOUT] " + bootstrapServersParam + " is: " + bootStrapServers);
			
			String topic = ConfigHandler.checkForNullConfigAndLoad(topicParam, conf);
			System.out.println("[KAFKA_SPOUT] " + topicParam + " is: " + topic);
			
			String consumerGroupId = ConfigHandler.checkForNullConfigAndLoad(consumerGroupParam, conf);
			System.out.println("[KAFKA_SPOUT] " + consumerGroupParam + " is: " + consumerGroupId);
			
			Long offsetCommitPeriodMs = Long
					.parseLong(ConfigHandler.checkForNullConfigAndLoad(offsetCommitPeriodMsParam, conf));
			
			System.out.println("[KAFKA_SPOUT] " + offsetCommitPeriodMsParam + " is: " + offsetCommitPeriodMs);
			
			int initialDelay = Integer
					.parseInt(ConfigHandler.checkForNullConfigAndLoad(initialDelayParam, conf));
			
			System.out.println("[KAFKA_SPOUT] " + initialDelayParam + " is: " + initialDelay);
			
			int delayPeriod = Integer
					.parseInt(ConfigHandler.checkForNullConfigAndLoad(delayPeriodParam, conf));
			
			System.out.println("[KAFKA_SPOUT] " + delayPeriodParam + " is: " + delayPeriod);
			
			
			int maxDelay = Integer
					.parseInt(ConfigHandler.checkForNullConfigAndLoad(maxDelayParam, conf));
			
			System.out.println("[KAFKA_SPOUT] " + maxDelayParam + " is: " + maxDelay);
			
			int maxUncommittedOffsets = Integer
					.parseInt(ConfigHandler.checkForNullConfigAndLoad(uncommittedOffsetsParam, conf));
			
			System.out.println("[KAFKA_SPOUT] " + uncommittedOffsetsParam + " is: " + maxUncommittedOffsets);

			System.out.println("[KAFKA_SPOUT] " + "Started initializing kafkaSpoutRetryService...");
			System.out.println("[KAFKA_SPOUT] " + "Initializing kafkaSpoutRetryService " + " with initial delay " + initialDelay
					+ " delay period " + delayPeriod + " max delay " + maxDelay);

			KafkaSpoutRetryService kafkaSpoutRetryService = new KafkaSpoutRetryExponentialBackoff(
					KafkaSpoutRetryExponentialBackoff.TimeInterval.microSeconds(initialDelay),
					KafkaSpoutRetryExponentialBackoff.TimeInterval.milliSeconds(delayPeriod), Integer.MAX_VALUE,
					KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(maxDelay));

			System.out.println("[KAFKA_SPOUT] " + "Finished initializing kafkaSpoutRetryService");

			String tupleFieldTopic = ConfigHandler.checkForNullConfigAndLoad(tupleFieldTopicParam, conf);
			System.out.println("[KAFKA_SPOUT] " + tupleFieldTopicParam + " is: " + tupleFieldTopic);
			
			String tupleFieldPartition = ConfigHandler.checkForNullConfigAndLoad(tupleFieldPartitionParam,conf);
			System.out.println("[KAFKA_SPOUT] " + tupleFieldPartitionParam + " is: " + tupleFieldPartition);
			
			String tupleFieldOffset = ConfigHandler.checkForNullConfigAndLoad(tupleFieldOffsetParam, conf);
			System.out.println("[KAFKA_SPOUT] " + tupleFieldOffsetParam + " is: " + tupleFieldOffset);
			
			
			String tupleFieldKey = ConfigHandler.checkForNullConfigAndLoad(tupleFieldKeyParam, conf);
			System.out.println("[KAFKA_SPOUT] " + tupleFieldKeyParam + " is: " + tupleFieldKey);
			
			String tupleFieldValue = ConfigHandler.checkForNullConfigAndLoad(tupleFieldValueParam, conf);
			System.out.println("[KAFKA_SPOUT] " + tupleFieldValueParam + " is: " + tupleFieldValue);
			
			boolean forceFromStart =  Boolean
					.parseBoolean(ConfigHandler.checkForNullConfigAndLoad(forceFromStartParam, conf));
			
			System.out.println("[KAFKA_SPOUT] " + forceFromStartParam + " is: " + forceFromStart);

			System.out.println("[KAFKA_SPOUT] " + "Started initializing spoutConf");
			Builder<String, String> spoutConfBuilder = KafkaSpoutConfig.builder(bootStrapServers, topic)
					.setProp(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId)
					.setOffsetCommitPeriodMs(offsetCommitPeriodMs)
					//.setFirstPollOffsetStrategy(KafkaSpoutConfig.FirstPollOffsetStrategy.UNCOMMITTED_LATEST)
					.setMaxUncommittedOffsets(maxUncommittedOffsets).setRetry(kafkaSpoutRetryService)
					.setRecordTranslator((r) -> new Values(r.topic(), r.partition(), r.offset(), r.key(), r.value()),
							new Fields(tupleFieldTopic, tupleFieldPartition, tupleFieldOffset, tupleFieldKey,
									tupleFieldValue));
			
			if(forceFromStart)
			{
				System.out.println("[KAFKA_SPOUT] " + "Forcing from start...");
				spoutConfBuilder.setFirstPollOffsetStrategy(KafkaSpoutConfig.FirstPollOffsetStrategy.EARLIEST);
	
			}
			else
			{
				System.out.println("[KAFKA_SPOUT] " + "Forcing from lateset uncomitted offset...");
				spoutConfBuilder.setFirstPollOffsetStrategy(KafkaSpoutConfig.FirstPollOffsetStrategy.UNCOMMITTED_LATEST);
			}
			
			KafkaSpoutConfig<String, String> spoutConf = spoutConfBuilder.build();
			
		
			
			System.out.println("[KAFKA_SPOUT] " + "Finished initializing spoutConf");

			builder.setSpout(spoutName, new KafkaSpout<String, String>(spoutConf), spoutParallelism);
			
			System.out.println("[KAFKA_SPOUT] " + "Finished setting kafka spout...");

		}

		String mapperBoltName = ConfigHandler.checkForNullConfigAndLoad("top.mapperbolt.name", conf);
		int mapperboltParallelism = Integer
				.parseInt(ConfigHandler.checkForNullConfigAndLoad("top.mapperbolt.parallelism", conf));

		System.out.println("Initializing " + mapperBoltName + " with parallelism " + mapperboltParallelism);
		builder.setBolt(mapperBoltName, new MapperBolt(), mapperboltParallelism).shuffleGrouping(spoutName);

	/*	String graphBoltName = ConfigHandler.checkForNullConfigAndLoad("top.graphbolt.name", conf);
		int graphBoltParallelism = Integer
				.parseInt(ConfigHandler.checkForNullConfigAndLoad("top.graphbolt.parallelism", conf));

		System.out.println("Initializing " + graphBoltName + " with parallelism " + graphBoltParallelism);
		builder.setBolt(graphBoltName, new JanusBolt(), graphBoltParallelism).shuffleGrouping(mapperBoltName);

		boolean debugMode = Boolean.getBoolean(ConfigHandler.checkForNullConfigAndLoad("top.debug", conf));
		conf.setDebug(debugMode);

		int numWorkers = Integer.parseInt(ConfigHandler.checkForNullConfigAndLoad("top.numWorkers", conf));
		conf.setNumWorkers(numWorkers);*/

		boolean localDeploy = Boolean.parseBoolean(ConfigHandler.checkForNullConfigAndLoad("top.localDeploy", conf));

		String topologyName = ConfigHandler.checkForNullConfigAndLoad("top.name", conf);

		if (localDeploy) {
			System.out.println("[DEPLOYER] " + " Submitting topology to local cluster...");
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(topologyName, conf, builder.createTopology());
		} 
		else
		{
			System.out.println("[DEPLOYER] " + " Submitting topology to remote cluster...");
			StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
		}

	}

	public static Config readConfigFromFile(String filename, Logger log) throws IOException {
		Config conf = new Config();
		System.out.println("[METRON] Reading config file: " + filename);

		FileInputStream fstream = new FileInputStream(filename);
		BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

		String strLine;

		while ((strLine = br.readLine()) != null) {
			System.out.println(strLine);

			if (strLine.length() != 0 && !(strLine.charAt(0) == '#')) {
				String[] parts = strLine.split("=");
				conf.put(parts[0], parts[1]);
				log.debug("Setting property " + parts[0] + " to " + parts[1]);
			}
		}

		br.close();

		return conf;
	}

}