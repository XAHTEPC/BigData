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

public class Task5 {
    public static class CityMapper extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = line.split(","); // предполагается, что данные разделены запятой
            String town = tokens[1].trim(); // предполагается, что town находится на втором месте
            if (town.equals("Tula")) {
                context.write(new Text(tokens[0]), value); // записываем id города и всю строку
            }
        }
    }
    public static class CityReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(null, value); // выводим все строки, удовлетворяющие условию
            }
        }
    }
    public static void start() throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "city filter");
        job.setJarByClass(Task5.class);
        job.setMapperClass(CityMapper.class);
        job.setReducerClass(CityReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("/home/caxapa/inputData/city.txt")); // входная директория
        FileOutputFormat.setOutputPath(job, new Path("/home/caxapa/outputData5")); // выходная директория
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
