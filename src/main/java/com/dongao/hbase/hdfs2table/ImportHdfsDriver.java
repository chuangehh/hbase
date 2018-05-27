package com.dongao.hbase.hdfs2table;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MutationSerialization;
import org.apache.hadoop.hbase.mapreduce.ResultSerialization;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * 根据HDFS中的数据导入到fruit_hdfs表中
 *
 * @author liangchuanchuan
 */
public class ImportHdfsDriver {

    public static void main(String[] args) throws Exception {
        // conf must create before the job
        Configuration conf = new Configuration();
        conf.set(TableOutputFormat.OUTPUT_TABLE, "fruit_hdfs");

        // get job instance
        Job job = Job.getInstance(conf, ImportHdfsDriver.class.getSimpleName());
        job.setJarByClass(ImportHdfsDriver.class);

        // input, set target fileDir
        FileInputFormat.setInputPaths(job, "hdfs://hadoop101:9000/input_fruit/fruit.tsv");
        job.setMapperClass(ImportHdfsMapper.class);

        // reduce
        job.setNumReduceTasks(0);

        // output
        job.setOutputFormatClass(TableOutputFormat.class);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Writable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}
