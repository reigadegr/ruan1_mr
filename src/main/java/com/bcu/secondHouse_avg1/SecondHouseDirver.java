package com.bcu.secondHouse_avg1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 二手房统计驱动类
 */
public class SecondHouseDirver extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        //构建Job对象
        Job job = Job.getInstance(super.getConf(), SecondHouseDirver.class.getSimpleName());
        job.setJarByClass(SecondHouseDirver.class);

        //input
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, new Path("/secondhouse"));

        //map
        job.setMapperClass(SecondHouseMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SecondHouseBean.class);

        //shuffle
        //设置分区器
        job.setPartitionerClass(SecondeHousePartition.class);
        //设置全局排序器
        job.setSortComparatorClass(SecondHouseSort.class);

        //reduce
        job.setReducerClass(SecondHouseReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(SecondHouseBean.class);
        //设置Reducer个数
        job.setNumReduceTasks(2);

        //output
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("/output/secondhouse_avg"));

        //submit
        return job.waitForCompletion(true) ? 0 : -1;
    }

    public static void main(String[] args) throws Exception {
        int status = ToolRunner.run(new Configuration(), new SecondHouseDirver(), args);
        System.exit(status);
    }
}
