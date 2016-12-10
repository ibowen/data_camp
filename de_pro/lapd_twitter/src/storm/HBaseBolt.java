package hbase;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class HBaseBolt extends BaseRichBolt {
	private static final long serialVersionUID = -5915311156387331493L;

	private static final Logger LOG = Logger.getLogger(HBaseBolt.class);

	private byte[] HBASE_CF;
//	private final byte[] COL_CITY = Bytes.toBytes("city");
//	private final byte[] COL_STATE = Bytes.toBytes("state");
//	private final byte[] COL_COUNTRY = Bytes.toBytes("country");

	private OutputCollector collector;
	private HTableFactoryInterface tableFactory;
	private HTableInterface eventsTable;

	public HBaseBolt(byte[] habse_cf, HTableFactoryInterface tableFactory) {
		this.tableFactory = tableFactory;
		HBASE_CF = habse_cf;
	}

	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		this.eventsTable = tableFactory.checkLapdTable();
	}

	// @Override
	public void execute(Tuple tuple) {
		List<Object> oneLine = tuple.getValues();
		try {
			Put put = constructRow(oneLine);
			this.eventsTable.put(put);
		} catch (Exception e) {
			LOG.error("Error inserting data into HBase table", e);
		}

		collector.emit(new Values(tuple.getString(0)));
		// acknowledge even if there is an error
		collector.ack(tuple);
	}

	private Put constructRow(List<Object> oneLine) {
		String rowKey = (String) oneLine.get(0);
		LOG.info("About to add row: " + rowKey);
		LOG.info("Number of lines : " + oneLine.size());
		LOG.info("First column value: " + oneLine.get(1));
		LOG.info("First column value: " + oneLine.get(2));
		LOG.info("First column value: " + oneLine.get(3));

		Put put = new Put(Bytes.toBytes(rowKey));
		put.add(HBASE_CF, Bytes.toBytes("retweeCount"), Bytes.toBytes((String) oneLine.get(1)));
		put.add(HBASE_CF, Bytes.toBytes("text"), Bytes.toBytes((String) oneLine.get(2)));
		put.add(HBASE_CF, Bytes.toBytes("createdAt"), Bytes.toBytes((String) oneLine.get(3)));
		return put;
	}

	// @Override
	public void cleanup() {
		try {
			eventsTable.close();
			tableFactory.cleanup();
		} catch (Exception e) {
			LOG.error("Error closing connections", e);
		}
	}

	// @Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("hbase_entry"));

	}

	// @Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	public static HBaseBolt make(Properties topologyConfig) {
		byte[] habse_cf = Bytes.toBytes(topologyConfig.getProperty("habse_cf"));
		return new HBaseBolt(habse_cf, new HTableFactory(topologyConfig));
	}
}
