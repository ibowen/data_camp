package hbase;

import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.log4j.Logger;

class HTableFactory implements HTableFactoryInterface, Serializable {
	private static final long serialVersionUID = -4565099913808078446L;
	private static final Logger LOG = Logger.getLogger(HTableFactory.class);
	private Properties topologyConfig;
	private HConnection connection;

	/**
	 * Table factory to create an HBase table object to access Hbase data The
	 * table and HBase settings are specified by topologyConfig
	 * 
	 * @param topologyConfig
	 *            set of properties to use to create the table
	 */
	HTableFactory(Properties topologyConfig) {
		this.topologyConfig = topologyConfig;
		this.connection = null;
	}

	/**
	 * Constructor that takes an HConnection object Used for unit testing so
	 * that HConnection can be mocked
	 * 
	 * @param topologyConfig
	 * @param connection
	 *            - mocked HConnection object
	 */
	HTableFactory(Properties topologyConfig, HConnection connection) {
		this.topologyConfig = topologyConfig;
		this.connection = connection;
	}

	/**
	 * Makes an HBase table object to property:lapd_hbase_table Verifies the
	 * expected table schema
	 */
	public HTableInterface checkLapdTable() {
		String hbaseSiteConfig = topologyConfig.getProperty("hbase_site");
		String hdfsSiteConfig = topologyConfig.getProperty("hdfs_site");
		String hdfsCoreSiteConfig = topologyConfig
				.getProperty("hdfs_core_site");
		String hbase_table_name = topologyConfig.getProperty("hbase_table");
		int EVENTS_NUM_CF = Integer.parseInt(topologyConfig
				.getProperty("habse_num_colFamilies"));
		String CF_EVENTS_TABLE_STR = topologyConfig.getProperty("habse_cf");

		Configuration hConf = HBaseConfiguration.create();
		hConf.addResource(new Path(hbaseSiteConfig));
		hConf.addResource(new Path(hdfsSiteConfig));
		hConf.addResource(new Path(hdfsCoreSiteConfig));

		HTableInterface table;
		try {
			if (connection == null) {
				connection = HConnectionManager.createConnection(hConf);
			}
			table = connection.getTable(hbase_table_name);
		} catch (Exception e) {
			String errMsg = "Error retrieving connection and access to HBase Tables";
			LOG.error(errMsg, e);
			throw new RuntimeException(errMsg, e);
		}

		HColumnDescriptor[] desc;
		// Once we have the table check that the column family exists
		try {
			desc = table.getTableDescriptor().getColumnFamilies();
		} catch (IOException e) {
			String eMsg = "Unable to get table description";
			LOG.error(eMsg, e);
			throw new RuntimeException(eMsg, e);
		}
		if (desc.length == EVENTS_NUM_CF) {
			// Right now our table only has one column family
			if (!desc[0].getNameAsString().equals(CF_EVENTS_TABLE_STR)) {
				String eMsg = "Column family definition is not as expected";
				LOG.error(eMsg);
				throw new RuntimeException(eMsg);
			}
		} else {
			String eMsg = "Number of column families not as expected";
			LOG.error(eMsg);
			throw new RuntimeException(eMsg);
		}
		LOG.info("Check hbase table OK : " + hbase_table_name);
		return table;
	}

	// Simply makes a connection to the lapd hbase_table
	public HTableInterface createLapdTable() {
		String hbaseSiteConfig = topologyConfig.getProperty("hbase_site");
		String hdfsSiteConfig = topologyConfig.getProperty("hdfs_site");
		String hdfsCoreSiteConfig = topologyConfig
				.getProperty("hdfs_core_site");
		String hbase_table_name = topologyConfig.getProperty("hbase_table");

		Configuration hConf = HBaseConfiguration.create();
		hConf.addResource(new Path(hbaseSiteConfig));
		hConf.addResource(new Path(hdfsSiteConfig));
		hConf.addResource(new Path(hdfsCoreSiteConfig));

		HTableInterface table;
		try {
			if (connection == null) {
				connection = HConnectionManager.createConnection(hConf);
			}
			table = connection.getTable(hbase_table_name);
		} catch (Exception e) {
			String errMsg = "Error retrieving connection and access to HBase Tables";
			LOG.error(errMsg, e);
			throw new RuntimeException(errMsg, e);
		}
		LOG.info("Connected hbase table: " + hbase_table_name);
		return table;
	}

	public void cleanup() {
		try {
			if (connection != null) {
				connection.close();
			}
		} catch (Exception e) {
			LOG.error("Error closing connections", e);
		}

	}
}
