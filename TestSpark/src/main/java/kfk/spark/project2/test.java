package kfk.spark.project2;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.List;

public class test extends Configured implements Tool{

    /**
     * map
     */
    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Text mapOutputKey = new Text();
        private IntWritable mapOutputValue = new IntWritable(1);
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            System.out.println("keyIn:"+key +"    ValueIn:"+value);
            String lineValue  = value.toString();
            String[] strs = lineValue.split(" ");
            for(String str : strs){
                mapOutputKey.set(str);
                context.write(mapOutputKey,mapOutputValue);
            }

        }
    }


    /**
     * reduce
     */
    public static class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable outputValue = new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            List<IntWritable> list = Lists.newArrayList(values);
            System.out.println("keyIn:"+key +"    ValueIn:"+list);
            int sum = 0;
            for(IntWritable value : list){
                sum +=value.get();
            }
            outputValue.set(sum);
            context.write(key,outputValue);
        }
    }


    /**
     * run
     * @param args
     * @return
     * @throws Exception
     */
    public int run(String args[]) throws Exception {

        //driver
        //1) get conf
        Configuration configuration = this.getConf();

        //2) create job
        Job job = Job.getInstance(configuration, this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());

        //3.1)  input
        Path path = new Path(args[0]);
        FileInputFormat.addInputPath(job, path);


        //3.2) map
        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //1.分区
        //job.setPartitionerClass();
        //2.排序
        //job.setSortComparatorClass();
        //3.分租
        //job.setGroupingComparatorClass();

        //3.3) reduce
        job.setReducerClass(WordCountReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //3.4) output
        Path output = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, output);

        //4) commit
        boolean isSuc = job.waitForCompletion(true);
        return (isSuc) ? 0 : 1;
    }

    public static void main(String[] args) {


        args = new String[]{
                "hdfs://bigdata-pro-m01.kfk.com:9000/user/kfk/datas/wordcount.txt",
                "hdfs://bigdata-pro-m01.kfk.com:9000/user/kfk/mr/output"
        };


        Configuration configuration = new Configuration();
        try {

            //先判断
            Path fileOutPath = new Path(args[1]);
            FileSystem fileSystem = FileSystem.get(configuration);
            if(fileSystem.exists(fileOutPath)){
                fileSystem.delete(fileOutPath,true);
            }

            //int status = wordCountMR.run(args);

            int status = ToolRunner.run(configuration,new test(),args);
            System.exit(status);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
