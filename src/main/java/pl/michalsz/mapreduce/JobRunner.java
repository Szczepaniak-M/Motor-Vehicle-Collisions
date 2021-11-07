package pl.michalsz.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JobRunner extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new JobRunner(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "pl.michalsz.JobRunner");
        job.setJarByClass(this.getClass());
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(CollisionMapper.class);
        job.setReducerClass(CollisionReducer.class);
        job.setCombinerClass(CollisionCombiner.class);
        job.setOutputKeyClass(CollisionData.class);
        job.setOutputValueClass(LongWritable.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
