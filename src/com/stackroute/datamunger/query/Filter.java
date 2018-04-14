package com.stackroute.datamunger.query;

import java.util.ArrayList;
import java.util.List;

import com.stackroute.datamunger.query.parser.Restriction;

//this class contains methods to evaluate expressions
public class Filter {

	/*
	 * the evaluateExpression() method of this class is responsible for
	 * evaluating the expressions mentioned in the query. It has to be noted
	 * that the process of evaluating expressions will be different for
	 * different data types. there are 6 operators that can exist within a query
	 * i.e. >=,<=,<,>,!=,= This method should be able to evaluate all of them.
	 * Note: while evaluating string expressions, please handle uppercase and
	 * lowercase
	 * 
	 */

	public Boolean evaluateExpression(Header header, RowDataTypeDefinitions dataTypeDefination, String[] data,
			List<Restriction> restriction, List<String> logicalOperators) {
		List<Boolean> list = new ArrayList<>();
		for (int index = 0; index < restriction.size(); index++) {
			if (restriction.get(index).getCondition().equals(">")) {
				int conditionValue = Integer.parseInt(restriction.get(index).getPropertyValue().trim());
				int rowDataValue = Integer.parseInt(data[header.get(restriction.get(index).getPropertyName().trim())]);
				list.add(conditionIsGreaterthan(rowDataValue, conditionValue));
			}
			if (restriction.get(index).getCondition().equals("<")) {
				int conditionValue = Integer.parseInt(restriction.get(index).getPropertyValue().trim());
				int rowDataValue = Integer.parseInt(data[header.get(restriction.get(index).getPropertyName().trim())]);
				list.add(conditionIsLessthan(rowDataValue, conditionValue));
			}
			if (restriction.get(index).getCondition().equals("<=")) {
				int conditionValue = Integer.parseInt(restriction.get(index).getPropertyValue().trim());
				int rowDataValue = Integer.parseInt(data[header.get(restriction.get(index).getPropertyName().trim())]);
				list.add(conditionIsLessthanOrEqual(rowDataValue, conditionValue));
			}
			if (restriction.get(index).getCondition().equals(">=")) {
				int conditionValue = Integer.parseInt(restriction.get(index).getPropertyValue().trim());
				int rowDataValue = Integer.parseInt(data[header.get(restriction.get(index).getPropertyName().trim())]);
				list.add(conditionIsGreaterthanOrEqual(rowDataValue, conditionValue));
			}
			if (restriction.get(index).getCondition().equals("!=")) {
				if (dataTypeDefination.get(header.get(restriction.get(index).getPropertyName().trim())).equals("java.lang.String")) {
					String conditionValue = restriction.get(index).getPropertyValue().trim().toLowerCase();
					String rowDataValue = data[header.get(restriction.get(index).getPropertyName().trim())].toLowerCase();
					list.add(conditionIsNotEqualTo(rowDataValue, conditionValue));
				}
				if (dataTypeDefination.get(header.get(restriction.get(index).getPropertyName().trim())).equals("java.lang.Integer")) {
					int conditionValue = Integer.parseInt(restriction.get(index).getPropertyValue().trim());
					int rowDataValue = Integer.parseInt(data[header.get(restriction.get(index).getPropertyName().trim())]);
					list.add(conditionIsNotEqualTo(rowDataValue, conditionValue));
				}
			}
			if (restriction.get(index).getCondition().equals("=")) {
				if (dataTypeDefination.get(header.get(restriction.get(index).getPropertyName().trim())).equals("java.lang.String")) {
					String conditionValue = restriction.get(index).getPropertyValue().toLowerCase().trim();
					String rowDataValue = data[header.get(restriction.get(index).getPropertyName().trim())].toLowerCase();
					list.add(conditionIsEqualTo(rowDataValue, conditionValue));
				}
				if (dataTypeDefination.get(header.get(restriction.get(index).getPropertyName().trim())).equals("java.lang.Integer")) {
					int conditionValue = Integer.parseInt(restriction.get(index).getPropertyValue().trim());
					int rowDataValue = Integer.parseInt(data[header.get(restriction.get(index).getPropertyName().trim())]);
					list.add(conditionIsEqualTo(rowDataValue, conditionValue));
				}
			}
		}
		String[] finalResult ;

		if(logicalOperators!=null){
			finalResult = new String[(logicalOperators.size()
				+ list.size())];
		}
		else
		{
			finalResult = new String[list.size()];
		}
		int logCount = 0, valueCount = 0;
		for (int finalValue = 0; finalValue < finalResult.length; finalValue++) {
			if (finalValue % 2 == 1) {
				finalResult[finalValue] = logicalOperators.get(logCount);
				logCount++;
			} else {
				finalResult[finalValue] = Boolean.toString((list.get(valueCount)));
				valueCount++;
			}
		}
		
		StringBuffer conditionalstring=new StringBuffer();
		for (int finalValue = 0; finalValue < finalResult.length;finalValue++) {
			conditionalstring.append(finalResult[finalValue]);
			if(!(finalValue+1==finalResult.length))
			{
				conditionalstring.append(",");
			}
		}
		
		String[] orSplitvalue=conditionalstring.toString().split("or");
		int iteratorValue=0;
		Boolean indicator=false;
		for(int iterateIndex=0;iterateIndex<orSplitvalue.length;iterateIndex++)
		{
			int tempValue=0;
			if(orSplitvalue[iterateIndex].contains("true") && orSplitvalue[iterateIndex].contains("false"))
			{
				tempValue=0;
			}
			else if(orSplitvalue[iterateIndex].contains("true"))
			{
				tempValue=1;
			}
			else if(orSplitvalue[iterateIndex].contains("false"))
			{
				tempValue=0;
			}
			iteratorValue+=tempValue;
		}
		if(iteratorValue>=1)
		{
			indicator=true;
		}
		else if(iteratorValue==0)
		{
			indicator=false;
		}
	
		return indicator;
	}

	// method containing implementation of equalTo operator
	Boolean conditionIsEqualTo(String a, String b) {
		if (a.equals(b)) {
			return true;
		} else {
			return false;
		}

	}
	
	Boolean conditionIsEqualTo(int a, int b) {
		if (a == b) {
			return true;
		} else {
			return false;
		}
	}

	// method containing implementation of notEqualTo operator
	Boolean conditionIsNotEqualTo(String a, String b) {
		if (!a.equals(b)) {
			return true;
		} else {
			return false;
		}

	}
	
	Boolean conditionIsNotEqualTo(int a, int b) {
		if (a != b) {
			return true;
		} else {
			return false;
		}
	}


	// method containing implementation of greaterThanOrEqualTo operator
	Boolean conditionIsGreaterthanOrEqual(int a, int b) {
		if (a >= b) {
			return true;
		} else {
			return false;
		}

	}

	// method containing implementation of lessThanOrEqualTo operator
	Boolean conditionIsLessthanOrEqual(int a, int b) {
		if (a <= b) {
			return true;
		} else {
			return false;
		}

	}

	// method containing implementation of greaterThan operator
	Boolean conditionIsGreaterthan(int a, int b) {
		if (a > b) {
			return true;
		} else {
			return false;
		}

	}

	// method containing implementation of lessThan operator
	Boolean conditionIsLessthan(int a, int b) {
		if (a < b) {
			return true;
		} else {
			return false;
		}

	}

}
