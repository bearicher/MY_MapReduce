package com.mapreduce.demo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 * @program: MY_MapReduce
 * @description: 分组统计手机号码
 * @author: xuyao
 * @create: 2019/07/24 15:21
 */
public class PhoneCount {

    public static class PhoneCountMapper extends Mapper<Object, Text, Text, IntWritable> {

        private static final IntWritable ONE = new IntWritable(1);
        private Text phoneNo = new Text();

        /**
         * @Description: 重写map方法
         * @Param: [key, value, context]
         * @return: void
         * @Author: xuyao
         * @Date: 2019/07/24
         */
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                this.phoneNo.set(itr.nextToken());
                context.write(this.phoneNo, ONE);
            }
        }

    }

    public static class PhoneCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        /**
         * @Description: 重写reduce方法
         * @Param: [key, values, context]
         * @return: void
         * @Author: xuyao
         * @Date: 2019/07/24
         */
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            IntWritable val;
            for (Iterator i = values.iterator(); i.hasNext(); sum += val.get()) {
                val = (IntWritable) i.next();
            }
            this.result.set(sum);
            context.write(key, this.result);
        }

    }

}