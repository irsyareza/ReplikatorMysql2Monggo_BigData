package com.bajau.replikator.mysql;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.bajau.replikator.DataReader;
import com.bajau.replikator.DataRecord;
import com.bajau.replikator.DataWriter;


public class MySQLSource implements DataReader, DataWriter {
	
	
	private String _mTableName = null;
	private String _mCols[] = null;


	public MySQLSource(String table, String cols[]) {
		_mTableName = table;
		_mCols =cols;
	}
	
	
	private Connection _mConnection;
	private Statement _mInsertStatement;
	

	@Override
	public void connect() throws Exception {
		// TODO Auto-generated method stub
		try {
	        try {
	            Class.forName("com.mysql.jdbc.Driver");
	        } catch (ClassNotFoundException ex) {
	            throw new IOException("Error loading driver");
	        }
	 
	        _mConnection = DriverManager.getConnection("jdbc:mysql://localhost:3306/sample", "sample", "sample");
	        _mConnection.setAutoCommit(true);
	    } catch (SQLException ex) {
	        throw new IOException(ex);
	    }
	}

	@Override
	public void close() throws Exception {
		// TODO Auto-generated method stub
		try {
	        _mConnection.close();
	    } catch (SQLException ex) {
	        throw new IOException(ex);
	    }
	}

	@Override
	public void writeRecord(DataRecord rec) throws IOException {
		// TODO Auto-generated method stub
		if (_mInsertStatement == null) {
	        try {
	            StringBuilder strBuff = new StringBuilder("INSERT INTO PERSON(");
	            StringBuilder strBuff2 = new StringBuilder(" VALUES(");
	            for (int i = 0; i < _mCols.length - 1; i++) {
	                strBuff.append(_mCols[i]).append(",");
	                strBuff2.append("?").append(",");
	            }
	            strBuff.append(_mCols[_mCols.length - 1]).append(") ");
	            strBuff2.append("?)");
	            strBuff.append(strBuff2.toString());
	 
	            _mInsertStatement = _mConnection.prepareStatement(strBuff.toString());
	        } catch (SQLException ex) {
	            throw new IOException(ex);
	        }
	    }
	 
	    try {
	        for(int i=0;i<_mCols.length;i++) {
	            ( (PreparedStatement) _mInsertStatement).setObject(i+1, rec.getDataAt(i));
	        }
	 
	        _mInsertStatement.executeBatch();
	    } catch (SQLException exp) {
	        throw new IOException(exp);
	    }
	}
	
	private ResultSet _mResultSet = null;

	public DataRecord readRecord() throws IOException {
		// TODO Auto-generated method stub
		if (_mResultSet == null) {
	        //Create SQL statemnt, execute the query and store the resultset instance.
	        StringBuilder strBuff = new StringBuilder("SELECT ");
	        for (int i = 0; i < _mCols.length - 1; i++) {
	            strBuff.append(_mCols[i]).append(",");
	        }
	         strBuff.append(_mCols[_mCols.length - 1]).append(" FROM ").append(_mTableName);
	        try {
	           _mResultSet = _mConnection.createStatement().executeQuery(strBuff.toString());
	        } catch (SQLException ex) {
	            throw new IOException(ex);
	        }
	    }
	 
	    //Fetch the record.
	    try {
	        if (!_mResultSet.next()) {
	            return null;
	        }
	 
	        Object data[] = new Object[_mCols.length];
	        for(int i=0;i<_mCols.length;i++) {
	            data[i] = _mResultSet.getObject(_mCols[i]);
	        }
	 
	        return new DataRecord(data);
	 
	    } catch (SQLException exp) {
	        throw new IOException(exp);
	    }
	}

}
