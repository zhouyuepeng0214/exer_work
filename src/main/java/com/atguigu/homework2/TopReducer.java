package com.atguigu.homework2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

public class TopReducer extends Reducer<FlowBean,Text,Text,FlowBean> {

    TreeMap<FlowBean,Text> flowMap = new TreeMap<>();

    protected void reduce(FlowBean key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for (Text value : values) {
            FlowBean bean = new FlowBean();
            bean.set(key.getDownFlow(),key.getUpFlow());
            flowMap.put(bean,new Text(value));
            if (flowMap.size() > 10) {
                flowMap.remove(flowMap.lastKey());
            }
        }
    }



    @Override
    protected void cleanup(Reducer<FlowBean, Text, Text, FlowBean>.Context context)
            throws IOException, InterruptedException {
        Iterator<FlowBean> it = flowMap.keySet().iterator();
        while (it.hasNext()) {
            FlowBean v = it.next();
            context.write(flowMap.get(v),v);
        }
    }
}
