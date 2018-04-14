package com.stackroute.datamunger.query.parser;

import java.util.ArrayList;
import java.util.List;

import com.stackroute.datamunger.DataMunger;

public class QueryParser extends DataMunger {

	/** To check the output main is added while doing the coding ***/
	/*
	 * public static void main(String[] args) { QueryParser q1= new
	 * QueryParser(); QueryParameter q2= new QueryParameter(); q2=
	 * q1.parseQuery(
	 * "select * from ipl.csv where season > 2014 and city ='Bangalore' group by save"
	 * ); System.out.println(q2.getRestrictions()); }
	 */
	private QueryParameter queryParameter = new QueryParameter();

	/*
	 * this method will parse the queryString and will return the object of
	 * QueryParameter class
	 */
	public QueryParameter parseQuery(String queryString) {

		/*
		 * extract the name of the file from the query. File name can be found
		 * after the "from" clause.
		 */
		queryParameter.setQueryString(queryString);
		queryParameter.setFile(getFile(queryString));

		queryParameter.setBaseQuery(getBaseQuery(queryString));

		/*
		 * extract the order by fields from the query string. Please note that
		 * we will need to extract the field(s) after "order by" clause in the
		 * query, if at all the order by clause exists. For eg: select
		 * city,winner,team1,team2 from data/ipl.csv order by city from the
		 * query mentioned above, we need to extract "city". Please note that we
		 * can have more than one order by fields.
		 */

		queryParameter.setOrderByFields(getOrderByFields(queryString));

		/*
		 * extract the group by fields from the query string. Please note that
		 * we will need to extract the field(s) after "group by" clause in the
		 * query, if at all the group by clause exists. For eg: select
		 * city,max(win_by_runs) from data/ipl.csv group by city from the query
		 * mentioned above, we need to extract "city". Please note that we can
		 * have more than one group by fields.
		 */

		queryParameter.setGroupByFields(getGroupByFields(queryString));

		/*
		 * extract the selected fields from the query string. Please note that
		 * we will need to extract the field(s) after "select" clause followed
		 * by a space from the query string. For eg: select city,win_by_runs
		 * from data/ipl.csv from the query mentioned above, we need to extract
		 * "city" and "win_by_runs". Please note that we might have a field
		 * containing name "from_date" or "from_hrs". Hence, consider this while
		 * parsing.
		 */

		queryParameter.setFields(getFields(queryString));

		/*
		 * extract the conditions from the query string(if exists). for each
		 * condition, we need to capture the following: 1. Name of field 2.
		 * condition 3. value
		 * 
		 * For eg: select city,winner,team1,team2,player_of_match from
		 * data/ipl.csv where season >= 2008 or toss_decision != bat
		 * 
		 * here, for the first condition, "season>=2008" we need to capture: 1.
		 * Name of field: season 2. condition: >= 3. value: 2008
		 * 
		 * the query might contain multiple conditions separated by OR/AND
		 * operators. Please consider this while parsing the conditions.
		 * 
		 */

		queryParameter.setRestrictions(getcondtionalfunctions(queryString));

		/*
		 * extract the logical operators(AND/OR) from the query, if at all it is
		 * present. For eg: select city,winner,team1,team2,player_of_match from
		 * data/ipl.csv where season >= 2008 or toss_decision != bat and city =
		 * bangalore
		 * 
		 * the query mentioned above in the example should return a List of
		 * Strings containing [or,and]
		 */

		queryParameter.setLogicalOperators(getLogicalOperators(queryString));

		/*
		 * extract the aggregate functions from the query. The presence of the
		 * aggregate functions can determined if we have either "min" or "max"
		 * or "sum" or "count" or "avg" followed by opening braces"(" after
		 * "select" clause in the query string. in case it is present, then we
		 * will have to extract the same. For each aggregate functions, we need
		 * to know the following: 1. type of aggregate
		 * function(min/max/count/sum/avg) 2. field on which the aggregate
		 * function is being applied
		 * 
		 * Please note that more than one aggregate function can be present in a
		 * query
		 * 
		 * 
		 */

		queryParameter.setAggregateFunctions(getAggregateFunctionsValue(queryString));
		return queryParameter;
	}

	// get and display the filename
	public String getFile(String queryString) {
		queryString = queryString.toLowerCase();
		if (queryString.contains("where")) {
			return queryString.split("from")[1].trim().split("where")[0].trim();
		} else {
			if ((queryString.contains("order by")) || (queryString.contains("group by"))) {
				return queryString.split("from")[1].split("order by|group by")[0].trim();
			} else {
				return queryString.split("from")[1].trim();
			}
		}
	}

	// getting the baseQuery and display
	// Checking whether the where condtion, group by and order by is there or
	// not and giving the base query
	public String getBaseQuery(String queryString) {

		if (queryString.contains("where")) {
			return queryString.split("where")[0].trim();
		} else if (queryString.contains("group by")) {
			return queryString.split("group by")[0].trim();
		} else if (queryString.contains("order by")) {
			return queryString.split("order by")[0].trim();
		} else {
			return null;
		}
	}

	// get and display the where conditions part(if where condition exists)
	public String getConditionsPartQuery(String queryString) {

		if (queryString.contains("where")) {
			if ((queryString.contains("order by")) || (queryString.contains("group by")))
				return queryString.split("where")[1].split("order by|group by")[0].toLowerCase();
			else
				return queryString.split("where")[1].toLowerCase();
		} else {
			return null;
		}
	}

	/*
	 * parse the where conditions and display the propertyName, propertyValue
	 * and conditionalOperator for each conditions
	 */

	public String[] getConditions(String queryString) {
		queryString = queryString.toLowerCase();
		if (queryString.contains("where")) {
			if ((queryString.contains("order by")) || (queryString.contains("group by"))) {
				String[] outputOfSplit = queryString.split("where")[1].trim().split(" order by | group by ")[0].trim()
						.split(" or |and");
				ArrayList<String> listOfConditions = new ArrayList<String>();
				for (String a : outputOfSplit) {
					listOfConditions.add(a.trim());
				}
				outputOfSplit = listOfConditions.toArray(outputOfSplit);
				return outputOfSplit;
			} else {
				String[] outputOfSplit = queryString.split("where")[1].trim().split(" or |and");
				ArrayList<String> listOfConditions = new ArrayList<String>();
				for (String a : outputOfSplit) {
					listOfConditions.add(a.trim());
				}
				outputOfSplit = listOfConditions.toArray(outputOfSplit);

				return outputOfSplit;

			}
		} else {
			return null;
		}
	}

	// get the logical operators(applicable only if multiple conditions exist)
	public ArrayList<String> getLogicalOperators(String queryString) {
		if ((queryString.contains("and")) || (queryString.contains("or")) || (queryString.contains("not"))) {
			String[] temp = queryString.split("\\s+");
			ArrayList<String> listOfOperators = new ArrayList<String>();
			for (String a : temp) {
				if (a.equals("and") || a.equals("or") || a.equals("not")) {
					listOfOperators.add(a);
				}
			}
			return listOfOperators;
		} else {
			return null;
		}

	}

	/* get the fields from the select clause */
	public ArrayList<String> getFields(String queryString) {

		String[] fieldsArray = queryString.split("from")[0].trim().split("select ")[1].split(",");
		ArrayList<String> listOfFields = new ArrayList<>();
		for (String var : fieldsArray) {
			listOfFields.add(var.trim());
		}
		return listOfFields;
	}

	// get order by fields if order by clause exists
	public ArrayList<String> getOrderByFields(String queryString) {

		if (queryString.contains("order by")) {
			String[] ordeyByFieldsArray = queryString.split("order by")[1].trim().split(",");
			ArrayList<String> listOfOrderByFields = new ArrayList<>();
			for (String var : ordeyByFieldsArray) {
				listOfOrderByFields.add(var.trim());
			}
			return listOfOrderByFields;
		} else {
			return null;
		}
	}

	// get group by fields if group by clause exists

	public ArrayList<String> getGroupByFields(String queryString) {

		if (queryString.contains("group by")) {
			if (queryString.contains("order by")) {

				String[] groupByFieldsArray = queryString.split("group by")[1].trim().split("order by")[0].split(",");
				ArrayList<String> listOfGroupByFields = new ArrayList<>();
				for (String var : groupByFieldsArray) {
					listOfGroupByFields.add(var.trim());
				}
				return listOfGroupByFields;

			} else {
				String[] groupByFieldsArray = queryString.split("group by")[1].trim().split(",");
				ArrayList<String> listOfGroupByFields = new ArrayList<>();
				for (String var : groupByFieldsArray) {
					listOfGroupByFields.add(var.trim());
				}
				return listOfGroupByFields;
			}
		}
		return null;
	}

	// parse and display aggregate functions(if applicable)

	public ArrayList<AggregateFunction> getAggregateFunctionsValue(String queryString) {

		String query = queryString.toLowerCase();
		String temp[] = null;
		String[] aggregateFunctions = new String[] { "min", "max", "count", "avg", "sum" };
		ArrayList<AggregateFunction> list = new ArrayList<AggregateFunction>();

		temp = query.split("from ")[0].trim().split("select")[1].trim().split(",");
		for (String i : temp) {
			for (String s : aggregateFunctions) {
				
					if (i.contains(s)) {
						AggregateFunction Aggregate = new AggregateFunction();
						Aggregate.setFunction(s.trim());
						Aggregate.setField(i.substring(i.indexOf("(") + 1, i.indexOf(")")).trim());
						list.add(Aggregate);
					}
				}
			}

		return list;
	}

	public List<Restriction> getcondtionalfunctions(String queryString) {
		// String query = queryString.toLowerCase();
		String query = queryString;
		String[] matches = new String[] { "<=", ">=", "!=", ">", "<", "=", "<>", "is ", "like ", "in ", };
		String[] conditionsArray = null;
		Boolean flag = false;
		List<Restriction> list = new ArrayList<>();
		if (query.contains("where ")) {
			if (query.contains("group by")) {
				query = query.split("group by")[0].trim();
			}
			if (query.contains("order by")) {
				query = query.split("order by")[0].trim();
				if (query.contains("group by")) {
					query = query.split("group by")[0].trim();
				}
			}
			conditionsArray = query.split("where ")[1].trim().split("and |or ");

			for (String k : conditionsArray) {
				Boolean conditions = true;
				for (String s : matches) {
					if (k.contains(s) && conditions) {
						Restriction Restrict = new Restriction();
						Restrict.setPropertyName(k.substring(0, k.indexOf(s)));
						Restrict.setCondition(s);
						if (s.equals(">=") || s.equals("<=") || s.equals("!=")) {
							conditions = false;
							Restrict.setPropertyValue(k.substring(k.indexOf(s) + 2, k.length()));
						} else {
							Restrict.setPropertyValue(k.substring(k.indexOf(s) + 1, k.length()));
						}

						list.add(Restrict);
						flag = true;

					}
				}
			}
		}
		if (flag == true)
			return list;
		else
			return null;
	}

}
