import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

public class testAvroParquetWriter {

    public static long blockSize = (1024 * 256);// 256 kb for local testing

    public static void main(String[] args) throws IOException {
        SparkSession sparkSession = SparkUtils.getSparkSession();
        final Dataset<Row> csv = sparkSession.read()
                .option("inferSchema", "true")
                .option("header", "true")
                .csv("src/test/resources/Record_5000_withID.csv");

        //Dataset<Row> ds = csv.withColumn("index", functions.monotonically_increasing_id());
        Dataset<Row> ds = csv.repartitionByRange(10, new Column("idx"));
        long count = ds.count();

        // write given partition on disk to estimate the dataset size
        String basePath = "src/test/resources/output/temp";
        UtilFuncs.WriteDatasetForEstimation(sparkSession, ds, basePath,
                40, count);

        System.out.println("end");
    }
}
