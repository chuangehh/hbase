package com.atguigu.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

/**
 * HBase api test
 *
 * @author liangchuanchuan
 */
public class HbaseApiTest {

    /**
     * hadoop 配置
     */
    protected Configuration conf;
    /**
     * 链接
     */
    protected Connection connection;

    /**
     * HBase操作接口
     */
    protected Admin admin;


    @Before
    public void init() throws IOException {
        conf = HBaseConfiguration.create();
        // set zookeeper servers
        conf.set("hbase.zookeeper.quorum", "hadoop101:2181,hadoop102:2181,hadoop103:2181");

        // get HBase connection
        connection = ConnectionFactory.createConnection(conf);
        // get HBase Admins
        admin = connection.getAdmin();
    }


    /**
     * table is Exist
     *
     * @param tableName
     * @return
     * @throws IOException
     */
    public boolean isTableExist(String tableName) throws IOException {
        return admin.tableExists(TableName.valueOf(tableName));
    }


    /**
     * create table
     *
     * @throws IOException
     */
    @Test
    public void createTable() throws IOException {
        // method params
        String tableName = "student";
        String[] columnFamily = new String[]{"info"};

        // business method
        if (isTableExist(tableName)) {
            throw new RuntimeException("表" + tableName + "已存在");
        } else {
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
            //创建多个列族
            for (String cf : columnFamily) {
                descriptor.addFamily(new HColumnDescriptor(cf));
            }
            admin.createTable(descriptor);
        }
    }

    /**
     * drop table
     *
     * @throws IOException
     */
    @Test
    public void dropTable() throws IOException {
        // method params
        String tableNameStr = "student";

        // business method
        if (isTableExist(tableNameStr)) {
            TableName tableName = TableName.valueOf(tableNameStr);
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
    }

    @Test
    public void addData() throws IOException {
        // method params
        String tableNameStr = "student";
        TableName tableName = TableName.valueOf(tableNameStr);

        // 使用该构造器能在 调用 hTable.close() 会把链接关掉
        HTable hTable = new HTable(conf, tableName);

        // 列族
        byte[] columnFamily = Bytes.toBytes("info");
        // 名字列
        byte[] nameColumn = Bytes.toBytes("name");
        // 年龄列
        byte[] ageColumn = Bytes.toBytes("age");

        Put put1 = new Put(Bytes.toBytes("1001"));
        put1.add(columnFamily, nameColumn, Bytes.toBytes("梁川川"));
        put1.add(columnFamily, ageColumn, Bytes.toBytes("18"));

        Put put2 = new Put(Bytes.toBytes("1002"));
        put2.add(columnFamily, nameColumn, Bytes.toBytes("赵繁旗"));
        put2.add(columnFamily, ageColumn, Bytes.toBytes("20"));
        hTable.put(Arrays.asList(put1, put2));

        // close
        hTable.close();
    }


}
