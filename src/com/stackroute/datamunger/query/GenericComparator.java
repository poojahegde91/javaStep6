package com.stackroute.datamunger.query;

import java.util.Comparator;

/*
 * The GenericComparator class implements Comparator Interface. This class is used to 
 * compare row objects which will be used for sorting the dataSet
 */
public class GenericComparator implements Comparator<Row> {

	private final String columnName;
	 Header header;
	 
	 RowDataTypeDefinitions dataTypes;
	 
	    public GenericComparator(Header header,RowDataTypeDefinitions dataTypes,String columnName) {
//	        super();
	    	this.header=header;
	    	this.dataTypes=dataTypes;
	        this.columnName = columnName;
	    }
	   
	@Override
	public int compare(Row o1, Row o2) {
		if(dataTypes.get(header.get(columnName)).trim().equals("java.lang.Integer"))
		{
		return Integer.parseInt(o1.get(columnName))-Integer.parseInt(o2.get(columnName));
		}
		else
		{
			return o1.get(columnName).toString().compareTo(o2.get(columnName).toString());
		}
}
		
}
