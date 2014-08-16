package com.spnotes.eshadoop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.elasticsearch.hadoop.mr.EsInputFormat;
import org.elasticsearch.hadoop.mr.WritableArrayWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MRInputDriver extends Configured implements Tool{
    Logger logger = LoggerFactory.getLogger(MRInputDriver.class);


	private static class MRInputMapper extends Mapper<Object, Object, Text, IntWritable>{
	    Logger logger = LoggerFactory.getLogger(MRInputMapper.class);
	    private static final IntWritable ONE = new IntWritable(1);

		@Override
		protected void map(Object key, Object value,
				Mapper<Object, Object, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			logger.debug("Entering MRInputDriver.map()");
			Text documentId = (Text)key;
			MapWritable valueMap = (MapWritable)value;
			
			
			WritableArrayWritable address =(WritableArrayWritable) valueMap.get(new Text("address"));
			MapWritable addressMap = (MapWritable)address.get()[0];
			Text city = (Text)addressMap.get(new Text("city"));
			
			context.write(city, ONE);
			
			logger.debug("Exiting MRInputDriver.map()");;
		}
	}

	private static class MRInputReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

	    Logger logger = LoggerFactory.getLogger(MRInputReducer.class);

		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			logger.debug("Entering MRInputReducer.reduce()");
			int sum = 0;
	        Iterator<IntWritable> valuesIt = values.iterator();
	        while(valuesIt.hasNext()){
	            sum = sum + valuesIt.next().get();
	        }
	        logger.debug(key + " -> " + sum);
	        context.write(key, new IntWritable(sum));
			
			logger.debug("Exiting MRInputReducer.reduce()");;
		}
	}
	
	

	public int run(String[] args) throws Exception {
		logger.debug("Entering MRInputDriver.run()");
		if (args.length != 2) {
            System.err.printf("Usage: %s [generic options] <input> <output>\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Job job = new Job();
        job.setJarByClass(MRInputDriver.class);
        job.setJobName("ContactImporter");
        
        logger.info("Input path " + args[0]);
        logger.info("Oupput path " + args[1]);

       // FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        Configuration configuration = job.getConfiguration();
        configuration.set("es.nodes","localhost:9200");
        configuration.set("es.resource",args[0]);
        job.setInputFormatClass(EsInputFormat.class);
        
   
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        job.setMapperClass(MRInputMapper.class);
        job.setReducerClass(MRInputReducer.class);

        int returnValue = job.waitForCompletion(true) ? 0:1;
        System.out.println("job.isSuccessful " + job.isSuccessful());
		logger.debug("Exiting MRInputDriver.run()");
		return returnValue;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MRInputDriver(), args);
        System.exit(exitCode);
	}

}
