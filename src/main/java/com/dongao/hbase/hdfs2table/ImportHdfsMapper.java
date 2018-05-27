package com.dongao.hbase.hdfs2table;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 导入mapper处理
 * 将fruit表中name通过MR迁入到fruit_mr表中
 *
 * @author liangchuanchuan
 */
public class ImportHdfsMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

    /**
     * info 列族
     */
    private final byte[] infoFamily = Bytes.toBytes("info");

    /**
     * name 列
     */
    private final byte[] nameColumn = Bytes.toBytes("info");

    /**
     * color 列
     */
    private final byte[] colorColumn = Bytes.toBytes("color");

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//从HDFS中读取的数据
        String lineValue = value.toString();
        //读取出来的每行数据使用\t进行分割，存于String数组
        String[] values = lineValue.split("\t");

        //根据数据中值的含义取值
        String rowKey = values[0];
        String name = values[1];
        String color = values[2];

        //初始化rowKey
        ImmutableBytesWritable rowKeyWritable = new ImmutableBytesWritable(Bytes.toBytes(rowKey));

        //初始化put对象
        Put put = new Put(Bytes.toBytes(rowKey));

        //参数分别:列族、列、值
        put.add(infoFamily, nameColumn, Bytes.toBytes(name));
        put.add(infoFamily, colorColumn, Bytes.toBytes(color));

        context.write(rowKeyWritable, put);
    }
}
