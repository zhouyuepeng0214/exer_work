package com.atguigu.homework33;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FFMapper2 extends Mapper<LongWritable, Text, Text, Text> {

    private Text k = new Text();
    private Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] split = value.toString().split("\t");

        v.set(split[0]);

        String[] persons = split[1].split(",");

        for (int i = 0; i < persons.length - 1; i++) {
            for (int j = i + 1; j < persons.length; j++) {
                if (persons[i].compareTo(persons[j]) < 0) {
                    k.set(persons[i] + "-" + persons[j]);
                } else  {
                    k.set(persons[j] + "-" + persons[i]);
                }
                context.write(k,v);
            }
        }
    }
}
