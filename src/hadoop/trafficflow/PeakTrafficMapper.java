package hadoop.trafficflow;

import java.io.IOException;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PeakTrafficMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable trafficCount = new IntWritable();
    private Text hourText = new Text();
    private SimpleDateFormat sdf = new SimpleDateFormat("hh:mm:ss a");

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        if (fields.length == 9) {
            try {
                Date date = sdf.parse(fields[0]);
                int hour = date.getHours();
                hourText.set(String.valueOf(hour));
                trafficCount.set(Integer.parseInt(fields[7]));
                context.write(hourText, trafficCount);
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }
}