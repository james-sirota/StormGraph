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
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff;
import org.apache.storm.kafka.spout.KafkaSpoutRetryService;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphTopology {

	private static final Logger logger = LoggerFactory.getLogger(GraphTopology.class);

	public static void main(String[] args) throws Exception {

		Config conf = readConfigFromFile(args[0]);

		TopologyBuilder builder = new TopologyBuilder();
		
		if(Boolean.parseBoolean(conf.get("top.generatorSpoutEnabled").toString()))
		{
		builder.setSpout(conf.get("top.spout.name").toString(), new TelemetryLoaderSpout(),
				Integer.parseInt(conf.get("top.spout.parallelism").toString()));
		}
		else
		{
			String bootStrapServers = conf.get("top.spout.kafka.bootStrapServers").toString();
			String topic = conf.get("top.spout.kafka.topic").toString();
			String consumerGroupId = conf.get("top.spout.kafka.consumerGroupId").toString();
			Long offsetCommitPeriodMs = Long.parseLong(conf.get("top.spout.kafka.consumerGroupId").toString());
			int initialDelay=Integer.parseInt(conf.get("top.spout.kafka.retry.initialDelay").toString());
			int delayPeriod=Integer.parseInt(conf.get("top.spout.kafka.retry.delayPeriod").toString());
			int maxDelay=Integer.parseInt(conf.get("top.spout.kafka.retry.maxDelay").toString());
			int maxUncommittedOffsets=Integer.parseInt(conf.get("top.spout.kafka.maxUncommittedOffsets").toString());
			
			KafkaSpoutRetryService kafkaSpoutRetryService =  new KafkaSpoutRetryExponentialBackoff(
					KafkaSpoutRetryExponentialBackoff.TimeInterval.microSeconds(initialDelay),
			        KafkaSpoutRetryExponentialBackoff.TimeInterval.milliSeconds(delayPeriod), 
			        Integer.MAX_VALUE, 
			        KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(maxDelay));
			
			KafkaSpoutConfig<String, String> spoutConf =  KafkaSpoutConfig.builder(bootStrapServers, topic)
			        .setProp(ConsumerConfig.GROUP_ID_CONFIG,consumerGroupId)
			        .setOffsetCommitPeriodMs(offsetCommitPeriodMs)
			        .setFirstPollOffsetStrategy(KafkaSpoutConfig.FirstPollOffsetStrategy.UNCOMMITTED_LATEST)
			        .setMaxUncommittedOffsets(maxUncommittedOffsets)
			        .setRetry(kafkaSpoutRetryService)
			        .setRecordTranslator((r) -> new Values(r.topic(), r.partition(), r.offset(), r.key(), r.value()),
			                new Fields("topic", "partition", "offset", "key", "value"))
			        .build();
			
			builder.setSpout(conf.get("top.spout.name").toString(), new KafkaSpout<String, String>(spoutConf),
					Integer.parseInt(conf.get("top.spout.parallelism").toString()));
		}

		builder.setBolt(conf.get("top.mapperbolt.name").toString(), new MapperBolt(),
				Integer.parseInt(conf.get("top.mapperbolt.parallelism").toString()))
				.shuffleGrouping(conf.get("top.spout.name").toString());

		builder.setBolt(conf.get("top.graphbolt.name").toString(), new JanusBolt(),
				Integer.parseInt(conf.get("top.graphbolt.parallelism").toString()))
				.shuffleGrouping(conf.get("top.mapperbolt.name").toString());

		conf.setDebug(Boolean.getBoolean("top.debug"));

		conf.setNumWorkers(Integer.parseInt(conf.get("top.numWorkers").toString()));

		boolean localDeploy = Boolean.parseBoolean(conf.get("top.localDeploy").toString());
		String topologyName = conf.get("top.name").toString();

		if (localDeploy) {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(topologyName, conf, builder.createTopology());
		} else {
			StormSubmitter.submitTopologyWithProgressBar(topologyName, conf, builder.createTopology());
		}

	}

	public static Config readConfigFromFile(String filename) throws IOException {
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
			}
		}

		br.close();

		return conf;
	}
}