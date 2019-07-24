import com.mapreduce.demo.WordCount;
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
 * @description: 单词计数运行类
 * @author: xuyao
 * @create: 2019/07/24 15:21
 */
public class WordCountTest {

    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "D:\\hadoop-common-2.7.3");
        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration, "WordCount");
        job.setJarByClass(WordCount.class);

        //设置Map和Reduce类
        job.setMapperClass(WordCount.WordCountMapper.class);
        job.setReducerClass(WordCount.WordCountReducer.class);

        //设置reduce输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置map输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        String[] otherArgs = new String[]{"input/essay.txt", "output1"};
        FileUtil.deleteDir(otherArgs[1], configuration);

        // 设置输入/输出数据存放位置
        FileInputFormat.setInputPaths(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }

}
