import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

public class test {

    public static long blockSize = (1024 * 256);// 256 kb for local testing

    public static void main(String[] args) throws IOException {
        SparkSession sparkSession = SparkUtils.getSparkSession();
        final Dataset<Row> csv = sparkSession.read()
                .option("inferSchema", "true")
                .option("header", "true")
                .csv("src/test/resources/Records_5000.csv");
        Dataset<Row> ds = csv.repartition(10);

        // write given partition on disk to estimate the dataset size
        String basePath = "src/test/resources/output/temp";
        final String tempPath = CustomRdd.WriteDatasetForEstimation(sparkSession, ds, basePath, 2);
        Dataset<Row> temp = sparkSession.read().parquet(tempPath);
        final long dirSize = SparkUtils.getDirSize(sparkSession, tempPath);
        long count = temp.count();
        System.out.println("file size :" + dirSize);
        System.out.println("record count:" + count);

        //calculation to estimate main  dataset size
        final long mainDatasetCount = ds.count();
        double predictedSize = (dirSize / (double) count) * (double) mainDatasetCount;
        System.out.println("predictedSize :" + predictedSize);

        final double calculatedDouble = (predictedSize / blockSize);
        final int calculatedPartition = (int) (Math.round(calculatedDouble));
        System.out.println("calculatedPartition" + calculatedDouble);

        ds.repartition(calculatedPartition).write()
                .option("compression", "snappy")
                .parquet("src/test/resources/output/fullds");

        System.out.println("end");
    }
}
