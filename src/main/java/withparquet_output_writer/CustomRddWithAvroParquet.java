package withparquet_output_writer;

import com.databricks.spark.avro.SchemaConverters;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.spark.Partition;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructType;
import scala.collection.Iterator;
import scala.collection.JavaConversions;
import scala.reflect.ClassTag;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class CustomRddWithAvroParquet extends RDD<Row> {
    Partition[] partition;
    ClassTag classTag;
    int noOfPartitions;
    StructType schema;
    long rowsToWrite;
    int startPartitionIndex;
    int endPartitionIndex;

    public CustomRddWithAvroParquet(RDD rdd, ClassTag classTag, Partition[] partition,
                                    long rowsToWrite, StructType schema, int startPartitionIndex, int endPartitionIndex) {
        super(rdd, classTag);
        this.classTag = classTag;
        this.partition = partition;
        this.noOfPartitions = noOfPartitions;
        this.schema = schema;
        this.rowsToWrite = rowsToWrite;

        //both inclusive in the range
        this.startPartitionIndex = startPartitionIndex;
        this.endPartitionIndex = endPartitionIndex;
    }

    @Override
    public Iterator<Row> compute(Partition partition, TaskContext taskContext) {

        final CustomPartition childPartition = (CustomPartition) partition;
        Iterator iterator = firstParent(classTag).iterator(childPartition.getPartition(), taskContext);

        UUID uuid = UUID.randomUUID();
        String basePath = "src/test/resources/output/temp1/";

        int cnt = 0;
        String recordName = "Model";
        String recordNamespace = "Test";
        SchemaBuilder.RecordBuilder<Schema> namespace =
                SchemaBuilder.record(recordName).namespace(recordNamespace);

        SchemaBuilder.RecordBuilder<Schema> namespace1 = SchemaBuilder.record(recordName).
                namespace(recordNamespace);

        Schema avroSchema = SchemaConverters.convertStructToAvro(this.schema, namespace1, "test");

        System.out.println(avroSchema.toString());
        String strTmp = System.getProperty("java.io.tmpdir");
        String p = basePath + uuid.toString();
        Path filePath = new Path(p);

        int blockSize = 1024;
        int pageSize = 65535;
        try {
            AvroParquetWriter parquetWriter = new AvroParquetWriter(
                    filePath,
                    avroSchema);
                    /*CompressionCodecName.SNAPPY);
                    blockSize,
                    pageSize);*/

            while (iterator.hasNext()) {
                Row next = (Row) iterator.next();
                GenericRecord gr = new GenericData.Record(avroSchema);
                for (int i = 0; i < next.length(); i++) {
                    gr.put(i, next.get(i));
                }
                parquetWriter.write(gr);
                cnt++;
            }
            parquetWriter.close();

        } catch (java.io.IOException e) {
            System.out.println(String.format("Error writing parquet file %s", e.getMessage()));
        }
        long size = getSize(p);
        Integer count = cnt;
        List<Row> list = new ArrayList<>();
        final Row row = RowFactory.create(count, size);
        list.add(row);

        return JavaConversions.asScalaIterator(list.iterator());
    }

    public long getSize(String filepath) {
        final java.nio.file.Path fpath = Paths.get(filepath);
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

    @Override
    public Partition[] getPartitions() {
        int min = Math.min(partition.length, endPartitionIndex);
        List<Partition> p = new ArrayList<>();
        int idx = 0;
        for (int i = startPartitionIndex; i <= min; i++) {
            p.add(new CustomPartition(partition[i], idx++));
        }
        return p.toArray(new Partition[0]);
    }
}
