import com.mapreduce.demo.AreaPartitioner;
import com.mapreduce.demo.PhoneCount;
import com.mapreduce.util.FileUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @program: MY_MapReduce
 * @description: 分组统计手机号码运行类
 * @author: xuyao
 * @create: 2019/07/24 15:32
 */
public class PhoneCountTest {

    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "D:\\hadoop-common-2.7.3");
        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration, "PhoneCount");
        job.setJarByClass(PhoneCount.class);

        // 设置Map,Reduce类和分组排序规则
        job.setMapperClass(PhoneCount.PhoneCountMapper.class);
        job.setPartitionerClass(AreaPartitioner.class);
        job.setReducerClass(PhoneCount.PhoneCountReducer.class);

        // 设置reduce映射数量,应该和分组数量一致
        job.setNumReduceTasks(4);

        // 设置reduce输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置map输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);


        String[] otherArgs = new String[]{"input/phone.txt", "output2"};
        FileUtil.deleteDir(otherArgs[1], configuration);

        // 设置输入/输出数据存放位置
        FileInputFormat.setInputPaths(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}