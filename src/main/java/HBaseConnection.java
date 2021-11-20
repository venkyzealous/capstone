import java.io.IOException;

import java.io.Serializable;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;

import org.apache.hadoop.hbase.client.Connection;

import org.apache.hadoop.hbase.client.ConnectionFactory;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Table;

public class HBaseConnection {
	private static final long serialVersionUID = 1L;

	private static Admin hbaseAdmin = null;
	private static Connection _connection = null;

	public static Admin getHbaseAdmin() throws IOException {

		try {

			if (hbaseAdmin == null)

			{

				org.apache.hadoop.conf.Configuration conf = (org.apache.hadoop.conf.Configuration) HBaseConfiguration
						.create();
				conf.setInt("timeout", 1200);
				conf.set("hbase.master", "ec2-54-165-166-214.compute-1.amazonaws.com:60000");
				conf.set("hbase.zookeeper.quorum", "ec2-54-165-166-214.compute-1.amazonaws.com");
				conf.set("hbase.zookeeper.property.clientPort", "2181");
				conf.set("zookeeper.znode.parent", "/hbase");
				Connection con = ConnectionFactory.createConnection(conf);

				// hbaseAdmin = new HBaseAdmin(conf);
				hbaseAdmin = con.getAdmin();

			}

		} catch (Exception e) {

			e.printStackTrace();

		}

		return hbaseAdmin;

	}
	
	public static Table getTable(String name) throws IOException {
		if(_connection == null) {
			org.apache.hadoop.conf.Configuration conf = (org.apache.hadoop.conf.Configuration) HBaseConfiguration
					.create();
			conf.setInt("timeout", 1200);
			conf.set("hbase.master", "ec2-54-165-166-214.compute-1.amazonaws.com:60000");
			conf.set("hbase.zookeeper.quorum", "ec2-54-165-166-214.compute-1.amazonaws.com");
			conf.set("hbase.zookeeper.property.clientPort", "2181");
			conf.set("zookeeper.znode.parent", "/hbase");
			_connection = ConnectionFactory.createConnection(conf);
		}
		
		return _connection.getTable(TableName.valueOf(name));
		
	}
	
}
