import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.net.URI;

public class Test {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        FileSystem fs = FileSystem.get(URI.create("hdfs://sandbox-hdp.hortonworks.com:8020/user/hadoop/spark/"), conf);



    }

}
