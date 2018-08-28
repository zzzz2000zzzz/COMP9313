package comp9313.ass1;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class EdgeAvgLen1
{
    
    public static class IntPair implements Writable
    {
        private int first;
		private float second;
        
        public IntPair() {}
        
        public IntPair(int first, float second)
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
    
    
    public static class TokenizerMapper extends Mapper<Object, Text, IntWritable, FloatWritable>
    {
    
        public void map(Object NodeID, Text value, Context context) 
                throws IOException, InterruptedException 
        {
            
            String itr[] = value.toString().split(" ");

            context.write(new IntWritable(Integer.parseInt(itr[2])),new FloatWritable(Float.parseFloat(itr[3])));
        }
    }
    

   
    public static class FloatSumReducer extends Reducer<IntWritable,FloatWritable,IntWritable,FloatWritable> 
    {
        private FloatWritable result = new FloatWritable();

        public void reduce(IntWritable NodeID, Iterable<FloatWritable> values, Context context) 
                throws IOException, InterruptedException 
        {
        	float sum_length = 0;
            float count = 0;
            for (FloatWritable value : values) 
            {
                sum_length += value.get();
                count += 1;
            }
            
            float avg = sum_length/count;
            result.set(avg);
            context.write(NodeID, result);   
        }
    }

    public static void main(String[] args) throws Exception 
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "EdgeAvgLen1");
        
        job.setNumReduceTasks(1);
        job.setJarByClass(EdgeAvgLen1.class);
        job.setMapperClass(TokenizerMapper.class);
        
        
        job.setCombinerClass(FloatSumReducer.class);
        job.setReducerClass(FloatSumReducer.class);
        
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(FloatWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}