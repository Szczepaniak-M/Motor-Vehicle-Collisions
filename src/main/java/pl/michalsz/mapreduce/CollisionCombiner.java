package pl.michalsz.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CollisionCombiner extends Reducer<CollisionData, LongWritable, CollisionData, LongWritable> {

    @Override
    public void reduce(CollisionData key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        LongWritable sum = new LongWritable(0);
        for (LongWritable val : values) {
            sum.set(sum.get() + val.get());
        }
        context.write(key, sum);
    }
}
