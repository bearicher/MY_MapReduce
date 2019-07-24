package com.mapreduce.demo;

import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;
import java.util.Map;

/**
 * @className: AreaPartitioner
 * @description: 自定义分组规则
 * @author: xuyao
 * @create: 2019/07/23 16:46
 */
public class AreaPartitioner<KEY, VALUE> extends Partitioner<KEY, VALUE> {

    private static Map<String, Integer> cacheValues = new HashMap<String, Integer>();

    static {
        cacheValues.put("153", 0);
        cacheValues.put("177", 1);
        cacheValues.put("147", 2);
    }

    /**
     * @Description: 从key中取出手机号, 不同字段返回不同组号
     * @Param: [key, value, numPartitions]
     * @return: int
     * @Author: xuyao
     * @Date: 2019/07/24
     */
    @Override
    public int getPartition(KEY key, VALUE value, int numPartitions) {
        Integer phoneNo = cacheValues.get(key.toString().substring(0, 3));
        return phoneNo == null ? 3 : phoneNo;
    }

}