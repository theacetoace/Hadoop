package org.myorg;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.Arrays;
import java.lang.Math;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Citation extends Configured implements Tool {
    
    public static class Map extends MapReduceBase implements Mapper<Text, Text, Text, Text> {
        
        public void map(Text key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String line = value.toString();
            if (line.startsWith("\"")) {
                return;
            }
            String[] result = line.split(",");
            output.collect(new Text(result[0]), new Text(result[3]));
        }
    }
    
    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            java.util.Map<String, Double> vals = new TreeMap<String, Double>();
            while (values.hasNext()) {
                String next = values.next().toString();
                if (vals.containsKey(next)) {
                    Double last = vals.get(next);
                    vals.put(next, last + 1);
                } else {
                    vals.put(next, 1.0);
                }
            }
            String res = (new Integer(vals.size())).toString();
            
            Double[] years = vals.values().toArray(new Double[vals.size()]);
            Arrays.sort(years);
            
            res = res + "\t" + years[0].toString();
            res = res + "\t" + years[years.length / 2].toString();
            res = res + "\t" + years[years.length - 1].toString();
            
            Double mean_val = 0.;
            for (int i = 0; i < years.length; i++) {
                mean_val += years[i];
            }
            
            mean_val /= years.length;
            
            res = res + "\t" + mean_val.toString();
            
            Double variance = 0.;
            
            for (int i = 0; i < years.length; i++) {
                variance += (years[i] - mean_val) * (years[i] - mean_val);
            }
            
            variance = Math.sqrt(variance / years.length);
            
            res = res + "\t" + variance.toString();
            
            output.collect(key, new Text(res));
        }
    }
    
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        JobConf job = new JobConf(conf, Citation.class);

        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);

        job.setJobName("patents");
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormat(KeyValueTextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.set("key.value.separator.in.input.line", ",");

        JobClient.runJob(job);
        
        return 0;
    }
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Citation(), args);

        System.exit(res);
    }
}
