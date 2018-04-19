
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class Rovkp {

    private static final String INTERMEDIATE_PATH = "intermediate";

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: Rovkp <input path> <output path>");
            System.exit(-1);
        }

        Long startTime = System.currentTimeMillis();

        Job aJob = Job.getInstance();
        aJob.setJarByClass(Rovkp.class);
        aJob.setJobName("Rovkp - aJob");

        FileInputFormat.addInputPath(aJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(aJob, new Path(INTERMEDIATE_PATH));


        aJob.setMapperClass(RovkpMapperFilter.class);
        aJob.setNumReduceTasks(0);

        aJob.setOutputKeyClass(NullWritable.class);
        aJob.setOutputValueClass(Text.class);

        aJob.waitForCompletion(true);


        int code = aJob.waitForCompletion(true) ? 0 : 1;

        if (code == 0) {
            Job bJob = Job.getInstance();
            bJob.setJarByClass(Rovkp.class);
            bJob.setJobName("Rovkp - bJob");

            FileInputFormat.addInputPath(bJob, new Path(INTERMEDIATE_PATH));
            FileOutputFormat.setOutputPath(bJob, new Path(args[1]));

            bJob.setMapperClass(RovkpMapper.class);
            bJob.setPartitionerClass(RovkpPartitioner.class);
            bJob.setReducerClass(RovkpReducer.class);
            bJob.setNumReduceTasks(24);

            bJob.setMapOutputKeyClass(Text.class);
            bJob.setMapOutputValueClass(RovkpWritable.class);
            bJob.setOutputKeyClass(NullWritable.class);
            bJob.setOutputValueClass(Text.class);

            code = bJob.waitForCompletion(true) ? 0 : 1;
        }

        Long endTime = System.currentTimeMillis();
        System.out.println("====> PASSED TIME: " + (endTime - startTime));

        System.exit(code);

    }
}
