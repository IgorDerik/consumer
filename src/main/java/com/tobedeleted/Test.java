package com.tobedeleted;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.File;
import java.net.URI;

public class Test {

    public static void main(String[] args) throws Exception {

        Configuration fsConf = new Configuration();
        fsConf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        fsConf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        FileSystem.get(URI.create(args[0]), fsConf);

        File file = new File(args[0]);

        System.out.println( file.exists() );
    }

}
