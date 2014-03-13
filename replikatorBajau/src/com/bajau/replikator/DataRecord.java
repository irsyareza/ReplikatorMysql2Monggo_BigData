package com.bajau.replikator;

public class DataRecord {
	
	private Object[] _mData = null;
	 
    public DataRecord(Object[] data) {
        _mData = data;
    }
 
    public Object getDataAt(int index) {
        return _mData[index];
    }

}
