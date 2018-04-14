package com.stackroute.datamunger.query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

//this class will be acting as the DataSet containing multiple rows
public class DataSet extends LinkedHashMap<Long, Row> {

	
	
	/*
	 * The sort() method will sort the dataSet based on the key column with the help
	 * of Comparator
	 */
	public DataSet sort(Header header, RowDataTypeDefinitions dataTypes, String columnName) {
		
		long rowIndex = 1;
		List<Row> dataList = new ArrayList<>(this.values());
		
		Collections.sort(dataList, new GenericComparator(header, dataTypes, columnName));
		DataSet dataSorted = new DataSet();
		for (Row dataRow : dataList) {
			dataSorted.put(rowIndex, dataRow);
			rowIndex++;
		}
		return dataSorted;

		
	}
	
}
