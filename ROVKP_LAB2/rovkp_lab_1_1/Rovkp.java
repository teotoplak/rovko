
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Rovkp {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: Rovkp <input path> <output path>");
            System.exit(-1);
        }

        Long startTime = System.currentTimeMillis();

        Job job = Job.getInstance();
        job.setJarByClass(Rovkp.class);
        job.setJobName("Rovkp");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(RovkpMapper.class);
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);

        Long endTime = System.currentTimeMillis();
        System.out.println("====> PASSED TIME: " + (endTime - startTime));

    }
}
