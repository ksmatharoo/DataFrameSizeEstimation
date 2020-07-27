
public class CustomPartitioner extends  org.apache.spark.Partitioner {
    @Override
    public int numPartitions() {
        return 0;
    }

    @Override
    public int getPartition(Object o) {
        return 0;
    }
}
