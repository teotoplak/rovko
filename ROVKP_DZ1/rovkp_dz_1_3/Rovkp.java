import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;

public class Rovkp {

    public static void main(String[] args) throws IOException, URISyntaxException {
        if (args.length != 2) {
            System.err.println("Expecting two arguments: <local_file> <hdfs_file>");
            return;
        }
        Path localFile = new Path(args[0]);
        Path hdfsFile = new Path(args[1]);

        Configuration configuration = new Configuration();
        LocalFileSystem local = LocalFileSystem.getLocal(configuration);
        FileSystem hdfs = FileSystem.get(new URI("hdfs://localhost:9000"), configuration);

        if (local.isFile(localFile)) {
            System.err.println("local path is valid and it's a file");
        } else if (local.isDirectory(localFile)) {
            System.err.println("local path is valid and it's a directory!");
        } else {
            System.err.println("local path doesn't exist!");
        }

        if (hdfs.isFile(hdfsFile)) {
            System.err.println("hdfs path is valid and it's a file");
        } else if (hdfs.isDirectory(hdfsFile)) {
            System.err.println("hdfs path is valid and it's a directory");
        } else {
            System.err.println("hdfs path doesn't exist!");
        }

    }
}
