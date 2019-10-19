package com.atguigu.homework22;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class TopNMapper extends Mapper<LongWritable,Text,FlowBean,NullWritable> {

    private FlowBean[] flows = new FlowBean[11];

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        for (int i =0 ;i < flows.length;i++) {
            flows[i] = new FlowBean();
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] fields = value.toString().split("\t");

        flows[10].set(Long.parseLong(fields[1]),
                Long.parseLong(fields[2]),
                fields[0]);
        Arrays.sort(flows);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (int i =0 ;i <10;i++) {
            context.write(flows[i],NullWritable.get());
        }
    }
}
