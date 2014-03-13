package com.bajau.replikator.mysql;

import java.io.IOException;

import com.bajau.replikator.DataReader;
import com.bajau.replikator.DataRecord;
import com.bajau.replikator.DataWriter;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class MongoDBSource implements DataReader, DataWriter{
	
	private String _mCollectionName = null;
    private String _mColumns[] = null;
 
    public MongoDBSource(String collectionName, String cols[]) {
        _mCollectionName = collectionName;
        _mColumns = cols;
    }
    
    MongoClient  _mClient;
    DBCollection _mCollection;
    DBCursor _mCursor;

	@Override
	public void connect() throws Exception {
		// TODO Auto-generated method stub
		_mClient = new MongoClient( "localhost" , 27017 );
		 
	    DB db = _mClient.getDB("sample");
	    _mCollection = db.getCollection(_mCollectionName);
	}

	@Override
	public void close() throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void writeRecord(DataRecord rec) throws IOException {
		// TODO Auto-generated method stub
		BasicDBObject doc = new BasicDBObject(_mColumns[0], rec.getDataAt(0));
	    for(int i=1;i<_mColumns.length;i++) {
	        doc = doc.append(_mColumns[i], rec.getDataAt(i));
	    }
	 
	    _mCollection.insert(doc);
	}

	@Override
	public DataRecord readRecord() throws IOException {
		// TODO Auto-generated method stub
		if(_mCursor == null) {
	        _mCursor = _mCollection.find();
	    }
	 
	    if(!_mCursor.hasNext()) {
	        return null;
	    }
	 
	    DBObject object = _mCursor.next();
	    Object values[] = new Object[_mColumns.length];
	    for(int i=0;i<_mColumns.length;i++) {
	        values[i] = object.get(_mColumns[i]);
	    }
	 
	    DataRecord rec = new DataRecord(values);
	    return rec;
	}

}
