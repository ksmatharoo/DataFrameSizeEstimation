package com.ksm.avroparquetwriter;

import org.apache.spark.Partition;

public class CustomPartition implements Partition {
    Partition partition;
    int index;

    public CustomPartition(Partition partition, int index) {
        this.partition = partition;
        this.index = index;
    }

    @Override
    public int index() {
        return index;
    }

    public Partition getPartition() {
        return partition;
    }
}
