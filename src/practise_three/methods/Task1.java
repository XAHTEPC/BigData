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
import java.util.HashMap;

public class Task1 {
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
                } else if (tableName.equals("Owner")) {
                    outputKey.set(id);
                    outputValue.set("O," + data);
                }

                context.write(outputKey, outputValue);
            }
        }
    }

    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String transportData = null;
            String ownerData = null;

            for (Text value : values) {
                String[] data = value.toString().split(",");
                if (data[0].equals("T")) {
                    transportData = data[1];
                } else if (data[0].equals("O")) {
                    if (ownerData == null) {
                        ownerData = "";
                    }
                    ownerData += data[1] + ",";
                }
            }

            if (transportData != null || ownerData != null) {
                if (ownerData != null) {
                    String[] owners = ownerData.split(",");
                    for (String owner : owners) {
                        context.write(new Text(key), new Text(transportData + "," + owner));
                    }
                }
//                else {
//                    context.write(new Text(key), new Text(transportData + ",null"));
//                }
            }
        }
    }
    public static void start() throws IOException, InterruptedException, ClassNotFoundException {
        ManageFile.task1();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "join transport owner");
        job.setJarByClass(Task1.class);

        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(JoinReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path("/home/caxapa/inputData/transport_owner.txt")); // Путь к входному файлу
        FileOutputFormat.setOutputPath(job, new Path("/home/caxapa/outputData1")); // Путь для выходных данных

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}