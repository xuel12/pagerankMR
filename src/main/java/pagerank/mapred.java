package pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.io.IOException;
import java.net.URI;


public class mapred extends Configured implements Tool {

    private static final Logger logger = LogManager.getLogger(mapred.class);
    public static final Integer MAX = 1000;
    public static final String cachefilename = "edge_synthetic.csv";
    public static final String cachefilepath = "/Users/xuel12/Documents/MSdatascience/CS6240parallel/hw4/pagerankMR";
//    public static final String cachefilepath = "s3://aws-logs-354124988676-us-east-1-twitter.triangle";

    public static class rankMapper extends Mapper<Object, Text, Text, DoubleWritable> {

        public final String DELIMITER = ",";
        private Map<String, ArrayList<String>> graphCache = new HashMap<>();

        @Override
        public void setup(Context context) throws IOException {

            try {
                FileSystem fs = FileSystem.get(new URI(cachefilepath+'/'), context.getConfiguration());
                Path edge_file_location=new Path(cachefilepath + "/inputedge/" + cachefilename);//Location of file in HDFS
//                FileSystem fs = FileSystem.get(new Configuration());
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(edge_file_location)));
                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    String[] field = line.split(DELIMITER, -1);
                    String follower = field[0];
                    String followee = field[1];
                    if (Integer.parseInt(follower)<=MAX && Integer.parseInt(followee)<=MAX) {
                        graphCache.computeIfAbsent(follower, k -> new ArrayList<>()).add(followee);
                    }
                }
                bufferedReader.close();
            } catch (Exception ex) {
                System.out.println(ex.getLocalizedMessage());
                String msg = "Did not specify files in distributed cache1";
                logger.error(msg);
                throw new IOException(msg);
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(DELIMITER);
            String follower = tokens[0];
            String rank = tokens[1];
            if (Integer.parseInt(follower)<=MAX) {
                // Ignore if the corresponding entry doesn't exist in the edge data (INNER JOIN)
                if (graphCache.get(follower) != null) {
                    int size = graphCache.get(follower).size();
                    Double contribution = Double.parseDouble(rank)/size;
                    for (String followee : graphCache.get(follower)) {
                        if (!followee.equals("0")) {
                            context.write(new Text(followee), new DoubleWritable(contribution));
                        } else {
                            for (String id : graphCache.keySet()) {
                                if (!id.equals("0")) {
                                    context.write(new Text(id), new DoubleWritable(contribution / (graphCache.keySet().size()-1)));
                                }
                            }
                            context.write(new Text("0"), new DoubleWritable(0.0));
                        }
                    }
                }
            }
        }
    }

    public static class path2Reducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0.0;
            for (DoubleWritable val : values) {
                sum += val.get();
            }
            if (key.toString().equals("0")) {
                result.set(0);
            } else {
                result.set(0.15/(MAX) + 0.85*(sum));
            }
            context.write(key, result);
        }
    }

    public static class iterMapper extends Mapper<Text, Text, Text, DoubleWritable> {

        private final static DoubleWritable one = new DoubleWritable(1.0);
        public final String DELIMITER = ",";
        private Map<String, ArrayList<String>> graphCache = new HashMap<>();

        @Override
        public void setup(Context context) throws IOException {

            try {
                FileSystem fs = FileSystem.get(new URI(cachefilepath+'/'), context.getConfiguration());
                Path edge_file_location=new Path(cachefilepath + "/inputedge/" + cachefilename);//Location of file in HDFS
//                FileSystem fs = FileSystem.get(new Configuration());
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(edge_file_location)));
                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    String[] field = line.split(DELIMITER, -1);
                    String follower = field[0];
                    String followee = field[1];
                    if (Integer.parseInt(follower)<=MAX && Integer.parseInt(followee)<=MAX) {
                        graphCache.computeIfAbsent(follower, k -> new ArrayList<>()).add(followee);
                    }
                }
                bufferedReader.close();
            } catch (Exception ex) {
                System.out.println(ex.getLocalizedMessage());
                String msg = "Did not specify files in distributed cache1";
                logger.error(msg);
                throw new IOException(msg);
            }
        }

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String follower = key.toString();
            String rank = value.toString();

            if (Integer.parseInt(follower)<=MAX) {
                // Ignore if the corresponding entry doesn't exist in the edge data (INNER JOIN)
                if (graphCache.get(follower) != null) {
                    int size = graphCache.get(follower).size();
                    Double contribution = Double.parseDouble(rank)/size;
                    for (String followee : graphCache.get(follower)) {
                        if (!followee.equals("0")) {
                            context.write(new Text(followee), new DoubleWritable(contribution));
                        } else {
                            for (String id : graphCache.keySet()) {
                                if (!id.equals("0")) {
                                    context.write(new Text(id), new DoubleWritable(contribution / (graphCache.keySet().size()-1)));
                                }
                            }
                            context.write(new Text("0"), new DoubleWritable(0.0));
                        }
                    }
                }
            }
        }
    }

    public int run(final String[] args) throws Exception {
        for (int i = 0; i < 10; i++) {
            // reuse the conf reference with a fresh object
            Configuration conf = getConf();
            // set the depth into the configuration
            conf.set("edge.file.path", cachefilepath);
            conf.set("edge.file.name", cachefilename);
            Job jobK = Job.getInstance(conf);
            FileSystem fs = FileSystem.get(conf);
            jobK.setJobName("iter " + i);

            // input output folder
            Path in;
            if (i < 1) {
                in = new Path(args[0]);
                jobK.setInputFormatClass(NLineInputFormat.class);
                jobK.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 50);
                jobK.setMapperClass(rankMapper.class);
            } else {
                in = new Path(args[1] + "/temp" + String.valueOf(i) + "/");
                jobK.setInputFormatClass(KeyValueTextInputFormat.class);
                jobK.setMapperClass(iterMapper.class);
            }
            FileInputFormat.addInputPath(jobK, in);

            Path out;
            if (i < 9) {
                out = new Path(args[1]+"/temp"+String.valueOf(i+1) + "/");
            } else {
                out = new Path(args[1]+"/final/");
            }
            // delete the outputpath if already exists
            if (fs.exists(out)) {
                fs.delete(out, true);
            }
            FileOutputFormat.setOutputPath(jobK, out);

            jobK.setReducerClass(path2Reducer.class);
            jobK.setJarByClass(mapred.class);

            jobK.setOutputKeyClass(Text.class);
            jobK.setOutputValueClass(DoubleWritable.class);
            jobK.waitForCompletion(true);
        }
        return(0);
    }

    public static void main(final String[] args) {
        if (args.length != 2) {
            throw new Error("Two arguments required:\n<input-dir> <output-dir>");
        }

        try {
            ToolRunner.run(new mapred(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
        System.exit(0);
    }
}