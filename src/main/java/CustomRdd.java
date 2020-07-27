import org.apache.spark.Partition;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import scala.collection.Iterator;
import scala.reflect.ClassManifestFactory$;
import scala.reflect.ClassTag;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class CustomRdd extends RDD<Row> {
    Partition[] partition;
    ClassTag classTag;
    int noOfPartitions;

    public CustomRdd(RDD rdd, ClassTag classTag, Partition[] partition, int noOfPartitions) {
        super(rdd, classTag);
        this.classTag = classTag;
        this.partition = partition;
        this.noOfPartitions = noOfPartitions;
    }

    static public String WriteDatasetForEstimation(SparkSession sparkSession, Dataset<Row> ds,
                                                   String basePath, int partitionUsed) {
        final StructType schema = ds.schema();
        ClassTag<Row> tag = ClassManifestFactory$.MODULE$.fromClass(Row.class);
        CustomRdd newRdd = new CustomRdd(ds.rdd(), tag,
                ds.javaRDD().partitions().toArray(new Partition[0]), partitionUsed);

        UUID uuid = UUID.randomUUID();
        //String basePath = "src/test/resources/output/temp";
        String tempPath = String.format("%s-%s", basePath, uuid.toString());

        sparkSession.createDataFrame(newRdd, schema)
                .write()
                .option("compression", "snappy")
                .parquet(tempPath);
        return tempPath;
    }

    @Override
    public Iterator<Row> compute(Partition partition, TaskContext taskContext) {
        final Iterator iterator = firstParent(classTag).iterator(partition, taskContext);
        return iterator;
    }

    @Override
    public Partition[] getPartitions() {
        int min = Math.min(partition.length, noOfPartitions);
        List<Partition> p = new ArrayList<>();
        for (int i = 0; i < min; i++) {
            p.add(partition[i]);
        }
        return p.toArray(new Partition[0]);
    }
}
