package com.model;

import java.io.Serializable;

public class TestPojo implements Serializable {
    int id;
    int partitioncol;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getPartitioncol() {
        return partitioncol;
    }

    public void setPartitioncol(int partitioncol) {
        this.partitioncol = partitioncol;
    }

    public TestPojo(int id, int partitionCol) {
        this.id = id;
        this.partitioncol = partitionCol;
    }
}
