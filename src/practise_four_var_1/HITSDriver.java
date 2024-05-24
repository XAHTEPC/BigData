package practise_four_var_1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HITSDriver {

    public static void start() throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "HITS");
        job.setJarByClass(HITSDriver.class);
        job.setMapperClass(HITSMapper.class);
        job.setReducerClass(HITSReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        HITSReducer.start();
        FileInputFormat.addInputPath(job,  new Path("/home/caxapa/inputData/inputHITS.txt"));
        FileOutputFormat.setOutputPath(job,  new Path("/home/caxapa/outputDataHITS"));
        job.waitForCompletion(true);
        Normalization.makeNormalization();
    }
}
