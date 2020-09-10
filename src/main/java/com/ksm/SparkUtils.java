package com.ksm;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.net.URI;

public class SparkUtils {

    public static SparkSession getSparkSession() {
        SparkConf conf = new SparkConf()
                .setAppName("testApp")
                .setMaster("local[1]");
        //SparkContext sc = new SparkContext(conf);

        return SparkSession.builder().config(conf).getOrCreate();
    }

    public static long getDirSize(SparkSession sparkSession, String p) throws IOException {
        Path path = new Path(p);
        URI uri = URI.create(p);
        FileSystem fs = FileSystem.get(uri, sparkSession.sparkContext().hadoopConfiguration());
        boolean directory = fs.getFileStatus(path).isDirectory();
        int len = 0;
        if (directory) {
            final RemoteIterator<LocatedFileStatus> rfs = fs.listFiles(path, false);
            while (rfs.hasNext()) {
                LocatedFileStatus next = rfs.next();
                if (next.getPath().getName().startsWith(".") ||
                        next.getPath().getName().contains("_SUCCESS")) {
                    continue;
                }
                len += next.getLen();
            }
        }
        return len;
    }
}
