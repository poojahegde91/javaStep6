package com.stackroute.datamunger.query;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/*
 * implementation of DataTypeDefinitions class. This class contains a method getDataTypes() 
 * which will contain the logic for getting the datatype for a given field value. This
 * method will be called from QueryProcessors.   
 * In this assignment, we are going to use Regular Expression to find the 
 * appropriate data type of a field. 
 * Integers: should contain only digits without decimal point 
 * Double: should contain digits as well as decimal point 
 * Date: Dates can be written in many formats in the CSV file. 
 * However, in this assignment,we will test for the following date formats('dd/mm/yyyy',
 * 'mm/dd/yyyy','dd-mon-yy','dd-mon-yyyy','dd-month-yy','dd-month-yyyy','yyyy-mm-dd')
 */
public class DataTypeDefinitions {
	
	private String[] headerNames;
	private String[] datatypes;
	
	public void setheaderNames(String[] headerNames) {
		this.headerNames = headerNames;
	}

	public void setdatatypes(String[] datatypes) {
		this.datatypes = datatypes;
	}

	public String[] getDataTypes(){
		int count = headerNames.length;
		String[] datatypesArray = new String[count];
		for (int index = 0; index < count; index++) {
			try {
				datatypesArray[index] = (String) getDataType(datatypes[index]);
			} catch (ArrayIndexOutOfBoundsException e) {
				datatypesArray[index] = (String) getDataType(" ");
			}
		}
		return datatypesArray;
	}

	public static Object getDataType(String input) {
		
		try {
			// checking for Integer
			Integer tempVariable = Integer.parseInt(input);
			return tempVariable.getClass().getName();
		} catch (Exception e) {
			try {
				String variable = input;
				java.util.Date date1 = null;
				// checking for date format dd/mm/yyyy
				if (variable.matches("([0-9]{2})-([0-9]{2})-([0-9]{4})")) {
					date1 = new SimpleDateFormat("dd-mm-yyyy").parse(variable);
					return date1.getClass().getName();
				}
				// checking for date format mm/dd/yyyy
				else if (variable.matches("([0-9]{2})-([0-9]{2})-([0-9]{4})")) {
					date1 = new SimpleDateFormat("mm-dd-yyyy").parse(variable);
					return date1.getClass().getName();
				}
				// checking for date format dd/mm/yy
				else if (variable.matches("([0-9]{2})-([0-9]{2})-([0-9]{2})")) {
					date1 = new SimpleDateFormat("dd-mm-yy").parse(variable);
					return date1.getClass().getName();
				}
				// checking for date format dd-mon-yy
				else if (variable.matches("([0-9]{2})-[a-zA-Z]{3}-([0-9]{2})")) {
					date1 = new SimpleDateFormat("dd-MMMM-yyyy").parse(variable);
					return date1.getClass().getName();
				}
				// checking for date format dd-mon-yyyy
				else if (variable.matches("([0-9]{2})-[a-zA-Z]{3}-([0-9]{4})")) {
					date1 = new SimpleDateFormat("dd-MMMM-yyyy").parse(variable);
					return date1.getClass().getName();
				}
				// checking for date format dd-month-yy
				else if (variable.matches("([0-9]{2})-[a-zA-Z]*-([0-9]{2})")) {
					date1 = new SimpleDateFormat("dd-MMMM-yyyy").parse(variable);
					return date1.getClass().getName();
				}
				// checking for date format dd-month-yyyy
				else if (variable.matches("([0-9]{2})-[a-zA-Z]*-([0-9]{4})")) {
					date1 = new SimpleDateFormat("dd-MMMM-yyyy").parse(variable);
					return date1.getClass().getName();
				}
				// checking for date format yyyy-mm-dd
				else if (variable.matches("([0-9]{4})-([0-9]{2})-([0-9]{2})")) {
					date1 = new SimpleDateFormat("yyyy-mm-dd").parse(variable);
					return date1.getClass().getName();
				}
				// Checking for float
				else {
					Float floatValue = Float.parseFloat(variable);
					return floatValue.getClass().getName();
				}
			} catch (Exception e1) {
				String tempVariable = input;
				if (tempVariable.trim().equals("")) {
					Object object = new Object();
					
					return object.getClass().getName();
				} else {
					return tempVariable.getClass().getName();
				}
			}
		}
	}
	
}
