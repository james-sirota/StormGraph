package metron.graph;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class JanusBolt extends BaseRichBolt {

	/**
	* 
	*/
	private static final long serialVersionUID = 3984660977031068498L;

	private String JANUS_CONFIG;
	private int TTL_VALUE;

	private JanusDAO jd;

	@SuppressWarnings("rawtypes")
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {

		JANUS_CONFIG = conf.get("top.graphbolt.backEndConfigLocation").toString();
		TTL_VALUE = Integer.parseInt(conf.get("top.graphbolt.ttlDays").toString());

		jd = new JanusDAO(JANUS_CONFIG, TTL_VALUE);
		// jd.createConnectsRelationshipSchema();
	}

	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String source = tuple.getString(0);
		String relation = tuple.getString(1);
		String dest = tuple.getString(2);
		String node1type = tuple.getString(3);
		String node2type = tuple.getString(4);

		jd.linkNodes(source, relation, dest, node1type, node2type);
	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub

	}

	public void execute(Tuple input) {
		// TODO Auto-generated method stub

	}

}
