package pl.michalsz.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CollisionMapper extends Mapper<LongWritable, Text, CollisionData, LongWritable> {

    private static final String PEDESTRIAN = "PEDESTRIAN";
    private static final String CYCLIST = "CYCLIST";
    private static final String MOTORIST = "MOTORIST";
    private static final String INJURED = "INJURED";
    private static final String KILLED = "KILLED";

    @Override
    public void map(LongWritable offset, Text lineText, Context context) {
        try {
            if (offset.get() != 0) {
                String line = lineText.toString();
                int i = 0;
                String zipCode = "";
                List<String> streets = new ArrayList<>();
                boolean isRowValid = true;
                for (String word : line.split(",")) {
                    switch (i) {
                        case 0:
                            int year = Integer.parseInt(word.substring(word.lastIndexOf('/') + 1, word.lastIndexOf('/') + 5));
                            if (year <= 2012) {
                                isRowValid = false;
                            }
                            break;
                        case 2:
                            if (word.equals("")) {
                                isRowValid = false;
                            }
                            zipCode = word;
                            break;
                        case 6:
                        case 7:
                        case 8:
                            if (!word.equals("")) {
                                streets.add(word.toUpperCase());
                            }
                            break;
                        case 11:
                            writeCollisionToContext(context, streets, zipCode, PEDESTRIAN, INJURED, word);
                            break;
                        case 12:
                            writeCollisionToContext(context, streets, zipCode, PEDESTRIAN, KILLED, word);
                            break;
                        case 13:
                            writeCollisionToContext(context, streets, zipCode, CYCLIST, INJURED, word);
                            break;
                        case 14:
                            writeCollisionToContext(context, streets, zipCode, CYCLIST, KILLED, word);
                            break;
                        case 15:
                            writeCollisionToContext(context, streets, zipCode, MOTORIST, INJURED, word);
                            break;
                        case 16:
                            writeCollisionToContext(context, streets, zipCode, MOTORIST, KILLED, word);
                            break;
                    }
                    if (!isRowValid) {
                        break;
                    }
                    i++;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void writeCollisionToContext(Context context, List<String> streets, String zipCode, String personType, String damageType, String word) throws IOException, InterruptedException {
        for (String street : streets) {
            CollisionData key = new CollisionData(street, zipCode, personType, damageType);
            LongWritable value = new LongWritable(Long.parseLong(word));
            context.write(key, value);
        }
    }
}
