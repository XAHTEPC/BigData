package practise_four_var_2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import practise_four_var_2.auth.HITSMapper_auth;
import practise_four_var_2.auth.HITSReducer_auth;
import practise_four_var_2.hub.HITSMapper_hub;
import practise_four_var_2.hub.HITSReducer_hub;

import java.io.IOException;

public class Main {
        public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
            Configuration conf = new Configuration();
            Job job = new Job(conf, "HITSA");
            job.setJarByClass(Main.class);
            job.setMapperClass(HITSMapper_auth.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setReducerClass(HITSReducer_auth.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path("/home/caxapa/inputData/inputHITS.txt"));
            FileOutputFormat.setOutputPath(job, new Path("/home/caxapa/outputDataHITS/ath_out"));
            if(job.waitForCompletion(true)) {
                job = new Job(conf, "NormA");
                job.setJarByClass(Main.class);
                job.setMapperClass(Normalization.CountMapperA.class);
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Text.class);
                job.setReducerClass(Normalization.CountReducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                FileInputFormat.addInputPath(job, new Path("/home/caxapa/outputDataHITS/ath_out/part-r-00000"));
                FileOutputFormat.setOutputPath(job, new Path("/home/caxapa/outputDataHITS/norma_out"));
                if(job.waitForCompletion(true)) {
                    job = new Job(conf, "JoinA");
                    job.setJarByClass(Main.class);
                    job.setMapperClass(Normalization.JoinAMapper.class);
                    job.setOutputKeyClass(Text.class);
                    job.setOutputValueClass(Text.class);
                    FileInputFormat.addInputPath(job, new Path("/home/caxapa/outputDataHITS/ath_out/part-r-00000"));
                    FileOutputFormat.setOutputPath(job, new Path("/home/caxapa/outputDataHITS/joina_out"));
                    if(job.waitForCompletion(true)){
                        job = new Job(conf, "HITSH");
                        job.setJarByClass(Main.class);
                        job.setMapperClass(HITSMapper_hub.class);
                        job.setMapOutputKeyClass(Text.class);
                        job.setMapOutputValueClass(Text.class);
                        job.setReducerClass(HITSReducer_hub.class);
                        job.setOutputKeyClass(Text.class);
                        job.setOutputValueClass(Text.class);
                        FileInputFormat.addInputPath(job, new Path("/home/caxapa/outputDataHITS/joina_out/part-r-00000"));
                        FileOutputFormat.setOutputPath(job, new Path("/home/caxapa/outputDataHITS/hub_out"));
                        if(job.waitForCompletion(true)){
                            job = new Job(conf, "NormH");
                            job.setJarByClass(Main.class);
                            job.setMapperClass(Normalization.CountMapperH.class);
                            job.setMapOutputKeyClass(Text.class);
                            job.setMapOutputValueClass(Text.class);
                            job.setReducerClass(Normalization.CountReducer.class);
                            job.setOutputKeyClass(Text.class);
                            job.setOutputValueClass(Text.class);
                            FileInputFormat.addInputPath(job, new Path("/home/caxapa/outputDataHITS/hub_out/part-r-00000"));
                            FileOutputFormat.setOutputPath(job, new Path("/home/caxapa/outputDataHITS/normh_out"));
                            if(job.waitForCompletion(true)){
                                System.out.println("OKOKOK");
                                job = new Job(conf, "JoinH");
                                job.setJarByClass(Main.class);
                                job.setMapperClass(Normalization.JoinHMapper.class);
                                job.setOutputKeyClass(Text.class);
                                job.setOutputValueClass(Text.class);
                                FileInputFormat.addInputPath(job, new Path("/home/caxapa/outputDataHITS/hub_out/part-r-00000"));
                                FileOutputFormat.setOutputPath(job, new Path("/home/caxapa/outputDataHITS/joinh_out"));
                                System.out.println("OKOKOK");
                                System.exit(job.waitForCompletion(true) ? 0 : 1);

                            }
                        }
                    }
                }
            }
        }
}
