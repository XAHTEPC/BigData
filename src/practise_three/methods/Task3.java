package practise_three.methods;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Task3 {

    public static class JoinMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Text outputKey = new Text();
        private Text outputValue = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");

            if (fields.length >= 3) {
                String tableName = fields[0];
                String id = fields[1];
                String data = fields[2];

                if (tableName.equals("Transport")) {
                    outputKey.set(id);
                    outputValue.set("T," + data);
                } else if (tableName.equals("City")) {
                    outputKey.set(id);
                    outputValue.set("C," + data);
                }

                context.write(outputKey, outputValue);
            }
        }
    }

    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String transportData = null;
            String cityData = null;

            for (Text value : values) {
                String[] data = value.toString().split(",");
                if (data[0].equals("T")) {
                    transportData = data[1];
                } else if (data[0].equals("C")) {
                    if (cityData == null) {
                        cityData = "";
                    }
                    cityData += data[1] + ",";
                }
            }

            if (transportData != null || cityData != null) {
                if (cityData != null) {
                    String[] cities = cityData.split(",");
                    for (String city : cities) {
                        context.write(new Text(key), new Text(transportData + "," + city));
                    }
                } else {
                    context.write(new Text(key), new Text(transportData + ",null"));
                }
            }
        }
    }

    public static void start() throws Exception {
        ManageFile.task3();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "right join transport city");
        job.setJarByClass(Task3.class);

        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(JoinReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job,new Path("/home/caxapa/inputData/transport_city.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/home/caxapa/outputData3")); // Путь для выходных данных

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}