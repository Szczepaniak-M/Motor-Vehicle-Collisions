package pl.michalsz.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CollisionReducer extends Reducer<CollisionData, LongWritable, Text, Text> {

    @Override
    public void reduce(CollisionData key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        LongWritable sum = new LongWritable(0);
        for (LongWritable val : values) {
            sum.set(sum.get() + val.get());
        }
        Text outputKey = new Text(key.toString());
        Text outputValue = new Text(key + "\t" + sum);
        context.write(outputKey, outputValue);
    }
}
