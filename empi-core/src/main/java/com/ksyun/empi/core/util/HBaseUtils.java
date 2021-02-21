package com.ksyun.empi.core.util;

import com.ksyun.empi.core.constants.ConstantValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class HBaseUtils {

    public static Connection getConnection(String zkQuorum, String port, String znode, String retries) throws Exception {

        Configuration conf = HBaseConfiguration.create();
        conf.set(ConstantValue.HBASE_ZOOKEEPER_QUORUM, zkQuorum);
        conf.set(ConstantValue.HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT, port);
        conf.set(ConstantValue.HBASE_ZOOKEEPER_ZNODE_PARENT, znode);
        conf.set(ConstantValue.HBASE_CLIENT_RETRIES_NUMBER, retries);

        return ConnectionFactory.createConnection(conf);
    }
}