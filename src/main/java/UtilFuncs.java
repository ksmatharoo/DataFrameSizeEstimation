import org.apache.hadoop.conf.Configuration;
import org.apache.spark.Partition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.SerializableConfiguration;
import scala.reflect.ClassTag;
import withavro_parquet_writer.CustomRDDParquetOutputWriter;
import withparquet_output_writer.CustomRddWithAvroParquet;

import java.util.ArrayList;

public class UtilFuncs {
    public static String WriteDatasetForEstimation(SparkSession sparkSession, Dataset<Row> ds,
                                                   String basePath, int percentageRowUsed, long totalRowCnt) {
        final StructType schema = ds.schema();
        //ClassTag<Row> tag1 = ClassManifestFactory$.MODULE$.fromClass(Row.class);
        ClassTag<Row> tag = ScalaUtils.getClassTag(Row.class);

        final ArrayList partitionList = new ArrayList(ds.javaRDD().partitions());
        Partition[] partitions = ds.javaRDD().partitions().toArray(new Partition[0]);
        long rowsToWrite = (long) (totalRowCnt * ((double) percentageRowUsed / 100));
        int startPartitionIndex = 0;
        int endPartitionIndex = 1;
        long size = 0;
        int rowCnt = 0;

        while (true) {
            CustomRddWithAvroParquet newRdd = new CustomRddWithAvroParquet(ds.rdd(), tag, partitions
                    , rowsToWrite, schema, startPartitionIndex, endPartitionIndex);

            final Object collect = newRdd.collect();
            Row[] rows = (Row[]) collect;
            for (int i = 0; i < rows.length; i++) {
                final int anInt = (int) (rows[i]).getInt(0);
                final long aLong = (rows[i]).getLong(1);
                size += aLong;
                rowCnt += anInt;
            }
            startPartitionIndex = endPartitionIndex + 1;
            endPartitionIndex = startPartitionIndex + 1;
            if (rowCnt >= rowsToWrite)
                break;
        }
        System.out.println("rowsToWrite " + rowsToWrite);
        System.out.println("Total rows used :" + rowCnt);
        System.out.println("Total size used :" + size);
        return "";
    }

    public static String WriteParquetOutputWriter(SparkSession sparkSession, Dataset<Row> ds,
                                                  String basePath, int percentageRowUsed, long totalRowCnt) {

        Configuration entries = sparkSession.sparkContext().hadoopConfiguration();
        ClassTag<Row> classTag = ScalaUtils.getClassTag(Row.class);
        CustomRDDParquetOutputWriter customRDD = new CustomRDDParquetOutputWriter(ds.rdd(), classTag,
                new SerializableConfiguration(entries), ds.javaRDD().partitions().toArray(new Partition[0]),
                ds.schema(), (int) totalRowCnt);
        customRDD.collect();

        return "";

    }


}