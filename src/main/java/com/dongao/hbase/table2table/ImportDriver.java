package com.dongao.hbase.table2table;

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

/**
 * 将fruit表中的一部分数据，通过MR迁入到fruit_mr表中
 *
 * @author liangchuanchuan
 */
public class ImportDriver {

    public static void main(String[] args) throws Exception {
        // conf must create before the job
        Configuration conf = new Configuration();
        conf.setStrings("io.serializations", conf.get("io.serializations"), MutationSerialization.class.getName(), ResultSerialization.class.getName());
        conf.set(TableOutputFormat.OUTPUT_TABLE, "fruit_mr");

        // get job instance
        Job job = Job.getInstance(conf, ImportDriver.class.getSimpleName());
        job.setJarByClass(ImportDriver.class);

        // input, set target table and scan
        TableMapReduceUtil.initTableMapperJob("fruit", new Scan(), ImportMapper.class, ImmutableBytesWritable.class, Put.class, job);

        // reduce
        job.setNumReduceTasks(0);

        // output
        job.setOutputFormatClass(TableOutputFormat.class);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Writable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}
