package com.bajau.replikator;

import java.io.IOException;

public interface DataReader extends DataContainer{
	
	public DataRecord readRecord() throws IOException;;

}
