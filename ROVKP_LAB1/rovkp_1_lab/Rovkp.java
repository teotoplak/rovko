import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

public class Rovkp {
    public static final String OUTPUT_FILE = "gutenberg_books.txt";


    public static void main(String[] args) throws IOException, URISyntaxException {


        if (args.length != 2) {
            System.err.println("Expecting two arguments: <local_directory> <hdfs directory>");
            return;
        }
        org.apache.hadoop.fs.Path localFile = new org.apache.hadoop.fs.Path(args[0] + OUTPUT_FILE);
        org.apache.hadoop.fs.Path hdfsFile = new org.apache.hadoop.fs.Path(args[1]);

        Configuration configuration = new Configuration();
        LocalFileSystem local = LocalFileSystem.getLocal(configuration);
        org.apache.hadoop.fs.FileSystem hdfs = org.apache.hadoop.fs.FileSystem.get(new URI("hdfs://cloudera2:8020"), configuration);


        hdfs.createNewFile(hdfsFile);

        RemoteIterator<LocatedFileStatus> iterator = local.listFiles(localFile, true);
        Integer linesNum = 0;
        Integer filesNum = 0;
        long startTime = System.currentTimeMillis();
        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(hdfs.create(hdfsFile)));
        while(iterator.hasNext()) {
            filesNum++;
            BufferedReader in = new BufferedReader(new InputStreamReader(local.open(iterator.next().getPath())));
            String line;
            while((line = in.readLine()) !=null) {
                out.write(line);
                linesNum++;
            }
            in.close();
        }

        out.close();
        local.close();
        hdfs.close();

        long elapsedTime = System.currentTimeMillis() - startTime;
        System.out.println("Number of lines processed: " + linesNum);
        System.out.println("Number of files processed: " + filesNum);
        System.out.println("Elapsed time in milliseconds: " + elapsedTime);

    }
}
