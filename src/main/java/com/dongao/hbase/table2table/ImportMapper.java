package com.dongao.hbase.table2table;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Arrays;

/**
 * 导入mapper处理
 * 将fruit表中name通过MR迁入到fruit_mr表中
 *
 * @author liangchuanchuan
 */
public class ImportMapper extends TableMapper<ImmutableBytesWritable, Put> {

    /**
     * info 列族
     */
    private final byte[] infoFamily = Bytes.toBytes("info");

    /**
     * name 列
     */
    private final byte[] nameColumn = Bytes.toBytes("info");

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        Put put = new Put(key.get());
        //遍历添加column行
        for (Cell cell : value.rawCells()) {
            // 同一个列族
            if (Arrays.equals(infoFamily, CellUtil.cloneFamily(cell))) {
                // name column
                put.add(new KeyValue(key.get(), infoFamily, nameColumn, CellUtil.cloneValue(cell)));
            }
        }
        context.write(key, put);
    }


}
