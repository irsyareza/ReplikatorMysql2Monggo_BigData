package com.bajau.replikator;

import java.io.IOException;

public interface DataWriter extends DataContainer {
 
    public void writeRecord(DataRecord rec) throws IOException;

}
