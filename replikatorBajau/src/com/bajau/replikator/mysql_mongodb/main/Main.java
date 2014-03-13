package com.bajau.replikator.mysql_mongodb.main;

import java.io.IOException;

import com.bajau.replikator.DataRecord;
import com.bajau.replikator.mysql.MongoDBSource;
import com.bajau.replikator.mysql.MySQLSource;

public class Main {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		String mySQLCols[] =  {"PERS_ID","PERS_DESC"};
		 
        //MongoDB columns
        String mongoDBCols[] =  {"id","desc"};
 
        MongoDBSource mg = new MongoDBSource("person",mongoDBCols);
        MySQLSource my = new MySQLSource("PERSON",mySQLCols);
 
        try {
            mg.connect();
            my.connect();
            DataRecord rec = my.readRecord();
            while(rec != null) {
                mg.writeRecord(rec);
                rec = my.readRecord();
            }
        } catch (IOException ex) {
            ex.printStackTrace();
            return;
        }
	}

}
