package com.ksm.parquetoutputwriter;

import org.apache.spark.Partition;

// merge all partition to one to run on single worker
public class MergePartition implements Partition {
    Partition[] partition;
    int index;

    public MergePartition(Partition[] partition, int index) {
        this.partition = partition;
        this.index = index;
    }

    @Override
    public int index() {
        return index;
    }

    public Partition[] getPartition() {
        return partition;
    }
}
