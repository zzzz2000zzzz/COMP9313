package comp9313.ass1;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import comp9313.ass1.EdgeAvgLen1.FloatSumReducer;
import comp9313.ass1.EdgeAvgLen1.TokenizerMapper;



public class EdgeAvgLen2
{
    
  
	 public static class FloatPair implements Writable
	    {
	        private int first;
			private float second;
	        
	        public FloatPair() {}
	        
	        public FloatPair(int first, float second)
	        {
	            set(first, second);
	        }
	        
	        public void set(int left, float right)
	        {
	            first = left;
	            second = right;
	        }
	        
	        public int getFirst()
	        {
	            return first;
	        }
	        
	        public float getSecond()
	        {
	            return second;
	        }
	        
	        public void write(DataOutput out) throws IOException
	        {
	            out.writeInt(first);
	            out.writeFloat(second);
	        }
	        
	        public void readFields(DataInput in) throws IOException
	        {
	            first = in.readInt();
	            second = in.readFloat();
	        }
	    }
    
   
    public static class TokenizerMapper extends Mapper<Object, Text, IntWritable, FloatPair>
    {
        private IntWritable word = new IntWritable();
        private HashMap<Integer, FloatPair> map = new HashMap<Integer, FloatPair>();
        
        public void setup(Context context) {}
        
        
        public void map(Object NodeID, Text value, Context context) 
                throws IOException, InterruptedException {
            
            String itr[] =  value.toString().split(" ");
            int key=Integer.parseInt(itr[2]);
            float val=Float.parseFloat(itr[3]);
            if (map.containsKey(key)){
            	FloatPair temp = map.get(key);
                temp.set(temp.getFirst()+1, temp.getSecond()+val);
                map.put(key, temp);
            }
            else{
            	FloatPair temp = new FloatPair();
                temp.set(1, val);
                map.put(key, temp);
            } 
        }
        public void cleanup(Context context) throws IOException, InterruptedException{
            for (int tmp : map.keySet())
            {
                word.set(tmp);
                context.write(word, map.get(tmp));
            }
        }
            
    }
    
    
    public static class FloatSumReducer extends Reducer<IntWritable,FloatPair,IntWritable,FloatWritable> 
    {
        private FloatWritable result = new FloatWritable();

        public void reduce(IntWritable NodeID, Iterable<FloatPair> values, Context context) 
                throws IOException, InterruptedException 
        {
        	float sum_length = 0;
            float count = 0;
            for (FloatPair value : values) 
            {
                sum_length += value.getSecond();
                count += value.getFirst();
            }
            
            float avg = sum_length/count;
            result.set(avg);
            context.write(NodeID, result);   
        }
    }
    
    public static void main(String[] args) throws Exception 
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "EdgeAvgLen2");
        
        job.setNumReduceTasks(1);
        job.setJarByClass(EdgeAvgLen2.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(FloatPair.class);
        
        
        job.setReducerClass(FloatSumReducer.class);
        
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(FloatWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    

}