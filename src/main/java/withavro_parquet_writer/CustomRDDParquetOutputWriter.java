package withavro_parquet_writer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.spark.Partition;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.execution.datasources.parquet.ParquetOutputWriter;
import org.apache.spark.sql.execution.datasources.parquet.ParquetWriteSupport;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.apache.spark.util.SerializableConfiguration;
import scala.collection.Iterator;
import scala.collection.JavaConversions;
import scala.reflect.ClassTag;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class CustomRDDParquetOutputWriter extends RDD<Row> {
    ClassTag<Row> tag;
    SerializableConfiguration conf;
    Partition[] partitions;
    StructType schema;
    int rowToWrite;

    public CustomRDDParquetOutputWriter(RDD<Row> oneParent, ClassTag<Row> tag,
                                        SerializableConfiguration conf, Partition[] partitions,
                                        StructType schema, int rowToWrite) {
        super(oneParent, tag);
        this.tag = tag;
        this.conf = conf;
        this.partitions = partitions;
        this.schema = schema;
        this.rowToWrite = rowToWrite;
    }

    @Override
    public Iterator<Row> compute(Partition partition, TaskContext taskContext) {
        final Partition[] partitions = ((MergePartition) partition).getPartition();


        final String tmpdir = System.getProperty("java.io.tmpdir");
        String basePath = "src/test/resources/output/temp1/";
        String path = basePath + UUID.randomUUID();
        System.out.println("outputPath " + path);
        Configuration confValue = conf.value();

        TaskAttemptID taskAttemptID = new TaskAttemptID("Test", 0, TaskType.MAP, 0, 0);

        JobConf jobConf = new JobConf(confValue);
        ParquetOutputFormat.setWriteSupportClass(jobConf, ParquetWriteSupport.class);
        ParquetWriteSupport.setSchema(schema, jobConf);
        jobConf.set(SQLConf.PARQUET_WRITE_LEGACY_FORMAT().key(), "false");
        jobConf.set(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE().key(), "INT96");
        //jobConf.set("mapreduce.output.fileoutputformat.outdir",path);

        TaskAttemptContextImpl context = new TaskAttemptContextImpl(jobConf, taskAttemptID);
        ParquetOutputWriter parquetOutputWriter = new ParquetOutputWriter(path, context);

        int rowCount = 0;
        for (Partition partition1 : partitions) {
            final Iterator<Row> iterator = firstParent(tag).iterator(partition1, taskContext);

            while (iterator.hasNext() && rowCount < rowToWrite) {
                Row next = iterator.next();
                List<Object> list = new ArrayList<>();
                for (int i = 0; i < next.size(); i++) {
                    if (next.get(i) instanceof String) {
                        list.add(UTF8String.fromString(next.getString(i)));
                    } else {
                        list.add(next.get(i));
                    }
                    rowCount++;
                }
                parquetOutputWriter.write(new GenericInternalRow(list.toArray()));
            }
            if (rowToWrite >= rowCount) {
                break;
            }
        }
        parquetOutputWriter.close();
        long size = getSize(path);

        List<Row> list = new ArrayList<>();
        Row row = RowFactory.create(rowCount, size);
        list.add(row);
        return JavaConversions.asScalaIterator(list.iterator());
    }

    @Override
    public Partition[] getPartitions() {
        //merge all partition to use on only single executor
        List<Partition> list = new ArrayList<>();
        list.add(new MergePartition(partitions, 0));
        return list.toArray(new Partition[0]);
    }

    public long getSize(String filepath) {
        final java.nio.file.Path fpath = java.nio.file.Paths.get(filepath);
        // size of a file (in bytes)
        long bytes = 0;
        try {
            bytes = Files.size(fpath);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(String.format("%,d bytes", bytes));
        System.out.println(String.format("%,d kilobytes", bytes / 1024));
        return bytes;
    }
}
