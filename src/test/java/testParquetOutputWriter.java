import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

public class testParquetOutputWriter {

    public static void main(String[] args) throws IOException {
        SparkSession sparkSession = SparkUtils.getSparkSession();
        final Dataset<Row> csv = sparkSession.read()
                .option("inferSchema", "true")
                .option("header", "true")
                .csv("src/test/resources/Record_5000_withID.csv");

        Dataset<Row> ds = csv.repartition(10);
        long count = ds.count();

        // write given partition on disk to estimate the dataset size
        String basePath = "src/test/resources/output/temp";
        UtilFuncs.WriteParquetOutputWriter(sparkSession, ds, basePath,
                45.0, count);

    }

}
