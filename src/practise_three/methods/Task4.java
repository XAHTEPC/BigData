package practise_three.methods;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Task4 {
    public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(","); // Предполагаем, что данные разделены запятой

            if (fields.length >= 2) {
                word.set(fields[1]); // Передаем значение столбца name в качестве ключа
                context.write(word, one); // Генерируем пару (ключ, значение) для передачи в Reducer
            }
        }
    }
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get(); // Суммируем все значения
            }
            result.set(sum);
            context.write(key, result); // Записываем результаты в контекст
        }
    }
    public static void start() throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "color count");
        job.setJarByClass(Task4.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("/home/caxapa/inputData/color.txt")); // входная директория
        FileOutputFormat.setOutputPath(job, new Path("/home/caxapa/outputData4")); // выходная директория
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}