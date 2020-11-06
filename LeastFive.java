package hw4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

import java.awt.List;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.TreeSet;


public class LeastFive {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: TopN <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf);
        job.setJobName("Top N");
        job.setJarByClass(LeastFive.class);
        job.setMapperClass(TopNMapper.class);
        //job.setCombinerClass(TopNReducer.class);
        job.setReducerClass(TopNReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


    public static class TopNMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private String tokens = "[_|$#<>\\^=\\[\\]\\*/\\\\,;,.\\-:()?!\"']";

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String cleanLine = value.toString().toLowerCase().replaceAll(tokens, " ");
            StringTokenizer itr = new StringTokenizer(cleanLine);
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken().trim());
                context.write(word, one);
            }
        }
    }


    public static class TopNReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private Map<Text, IntWritable> countMap = new HashMap<>();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            countMap.put(new Text(key), new IntWritable(sum));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

            Map<Text, IntWritable> sortedMap = sortByValues(countMap);

            int counter = 0;
            for (int i = sortedMap.size()-6; i<sortedMap.size()-1; i++) {
                if (counter ++ == 5) {
                    break;
                }
                Text key = (Text)sortedMap.keySet().toArray()[i];
                context.write(key, sortedMap.get( key));
            }
        }
    
    
    public static <K extends Comparable, V extends Comparable> Map<K, V> sortByValues(Map<K, V> map) {
        LinkedList<Map.Entry<K, V>> entries = new LinkedList<Map.Entry<K, V>>(map.entrySet());

        Collections.sort(entries, new Comparator<Map.Entry<K, V>>() {

            @Override
            public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
                return o2.getValue().compareTo(o1.getValue());
            }
        });


        Map<K, V> sortedMap = new LinkedHashMap<K, V>();

        for (Map.Entry<K, V> entry : entries) {
            sortedMap.put(entry.getKey(), entry.getValue());
        }

        return sortedMap;
    }
    }
    }

//
// public static void main(String[] args) throws Exception {
//   Configuration conf = new Configuration();
//   GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
//   String[] remainingArgs = optionParser.getRemainingArgs();
//   if ((remainingArgs.length != 2) && (remainingArgs.length != 4)) {
//     System.err.println("Usage: wordcount <in> <out> [-skip skipPatternFile]");
//     System.exit(2);
//   }
//   Job job = Job.getInstance(conf, "word count");
//   job.setJarByClass(LeastFive.class);
//   job.setMapperClass(TokenizerMapper.class);
//  job.setCombinerClass(IntSumReducer.class);
//   job.setReducerClass(IntSumReducer.class);
//	//job.setNumReduceTasks(1);
//   job.setOutputKeyClass(Text.class);
//   job.setOutputValueClass(IntWritable.class);
//
//   ArrayList<String> otherArgs = new ArrayList<String>();
//   for (int i=0; i < remainingArgs.length; ++i) {
//     if ("-skip".equals(remainingArgs[i])) {
//       job.addCacheFile(new Path(remainingArgs[++i]).toUri());
//       job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
//     } else {
//       otherArgs.add(remainingArgs[i]);
//     }
//   }
//   FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
//   FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(1)));
//
//   System.exit(job.waitForCompletion(true) ? 0 : 1);
// }
//}
