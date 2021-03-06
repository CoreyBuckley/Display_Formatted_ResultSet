/*
 * 		WHAT I LEARNED FROM MAKING THIS:
 * 
 * 		Executing another operation with the Statement object will automatically close the previous ResultSet if one was created.
 * 		Closing a Statement object will also close any ResultSet associated with it.
 * 
 * 		You can use the try-with construct where you put objects implementing the AutoClosable interface in parenthesizes separated by semicolons.
 * 		This makes it easier to release the resources of an object, even if an Exception is encountered, the object will be closed.
 * 			try (Scanner input = new Scanner(System.in)) {
 * 
 * 			} catch (Exception e) { }
 * 
 * 		You can use printf to print an Array of varargs. Varargs is basically an Object[], so if you have a String[] then you can pass that in because a String
 * 		inherits from Object[]. Similarly, you could pass in an array of 'primitives' if you used the associated boxing class (i.e. Integer). 
 */

package dbms20;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.HashMap;

/**
 * 		DisplayResultSet is a static class used for displaying the contents of a ResultSet to the console.
 * 
 * 		You can set a format for your display either through an Enum of the class or a custom string of format specifiers like "%12s%-50s" etc.
 * 		All formats provided in the print function will be stretched to the number of columns in your ResultSet.
 * 		If you omit a format then the class will automatically use a singular format for all columns of the result set. 
 * 		You can change this default by setting the default format or passing in only one format specifier to the print function.
 * 		
 * 		@author Corey Buckley
 * 		@version 1.0
 * 		@since 04/09/2018
 * 
 */
public final class DisplayResultSet {
	
	public enum Align {LEFT, RIGHT}; //maybe add center one day
	
	//The defaultFormat ideally should be set to the absolute format of the table, that is, a string of
	//format specifiers in order of the columns. 
	//This format will then be rearranged to match the current columns of a ResultSet
	private static String defaultFormat = "%-20s"; //used if a format is not specified
	
	
	/**
	 * Takes a {@code ResultSet} and a {@code String} of format specifiers. This {@code static} function will print
	 * the contents of a {@code ResultSet} to the console using the passed in <b>headerFormat</b> where each format 
	 * specifier corresponds to the columns from left to right. 
	 * The <b>headerFormat</b> should correspond to all columns of <b>results</b>. 
	 * <br><br>
	 * <b>Note: </b>The number of format specifiers should match the number of columns in the ResultSet.
	 * 
	 * @param results
	 * @param headerFormat
	 * @throws SQLException
	 * 
	 * This function will print the contents of a ResultSet with the format specifier(s) passed in, 
	 * where each specifier corresponds to a table column.
	 */
	public static void print(ResultSet results, String headerFormat) throws SQLException { 
		ResultSetMetaData metadata = results.getMetaData();
		final String[] localColumnNames = getLocalColumnNames(metadata);
		String[] rowValues;
		headerFormat = stretchPattern(localColumnNames.length, headerFormat); //in the case that the passed in format is < columns, stretch the format to match
		System.out.printf(headerFormat, localColumnNames);
		System.out.println(); //line between the header and the records
		while(results.next()) {
			rowValues = getRowValues(results, localColumnNames);
			System.out.printf(headerFormat, rowValues);
		} 
	}
	
	/**
	 * Takes a {@code Statement} object separate from what generated the passed in {@code ResultSet}. The <b>stmt</b> object is used to
	 * get all the columns of the {@code ResultSet} so that the default format can be rearranged to match the {@code ResultSet}'s contents, 
	 * given that the default format has more than 1 unique format specifier.
	 * 
	 * @param stmt
	 * @param results
	 * @param headerFormat
	 * @throws SQLException
	 * 
	 * This function will print the contents of a ResultSet with the format specifier(s) passed in, where each specifier corresponds to a table column.
	 */
	public static void print(Statement stmt, ResultSet results) throws SQLException { 
		long begin = System.nanoTime();
		ResultSetMetaData metadata = results.getMetaData();
		ResultSet all = stmt.executeQuery("SELECT * FROM Comics");
		String headerFormat = defaultFormat;
		final String[] localColumnNames = getLocalColumnNames(metadata);
		final String[] absoluteColumnNames = getAbsoluteColumnNames(all);
		final int maxColumnIndex = getMaxiumumColumnIndex(all, localColumnNames);
		String[] rowValues;
		String[] specifiers = getFormatSpecifiers(headerFormat);
		if (specifiers.length > 1) { //if we only have one format specifier, then there's no need to rearrange the format
			HashMap<String,String> columnNameToFormatting = bindColumnNameToFormatPattern(absoluteColumnNames, specifiers, maxColumnIndex); 
			headerFormat = getRearrangedFormattedHeader(localColumnNames, columnNameToFormatting);
		}
		headerFormat = stretchPattern(localColumnNames.length, defaultFormat);
		System.out.printf(headerFormat, localColumnNames);	
		System.out.println("\n"); //2 lines between the header and the records
		while(results.next()) {
			rowValues = getRowValues(results, localColumnNames);
			System.out.printf(headerFormat, rowValues);
			System.out.println();
		} 
		stmt.close(); //releasing the resources of the statement also closes the ResultSet (according to doc)
		long end = System.nanoTime();
		long duration = (end-begin)/1000000; //divide by a million is milliseconds
		System.out.println("\n\n\n" + duration + "ms elapsed" );
	}
	
	//produces a format specifier for a string.
	public static void setDefaultFormat(int spaces, Align alignment) { 
		defaultFormat = "%" + (alignment==Align.LEFT ? "-" : "") + spaces + "s"; //the default alignment for a format specifier is right-justified (e.g. %20s)
	}
	
	public static void setDefaultFormat(String s) { //let the user input a format specifier. Could rework this to validate input
		defaultFormat = s;
	}
	
	//uses the metadata of a ResultSet in order to get columnCount and and loop through the table (linear) and 
	//get the column name with getColumnName(). Can print the columns with prinf() easily when in a String[] 
	//and access the column names programmatically when we need to get row values
	private static String[] getLocalColumnNames(ResultSetMetaData metadata) throws SQLException { //will get the column names in the ResultSet
		int columnCount = metadata.getColumnCount();
		String[] columnNames = new String[columnCount];
		for (int i = 0; i < columnCount; i++) {
			columnNames[i] = metadata.getColumnName(i+1);
		}
		return columnNames;
	}
	
	//Gets the column names of the whole table by using a ResultSet that came from "SELECT * FROM ..."
	//Purpose of this method is to eventually bind the column name to a format in a HashMap so we can
	//get the format of a column. This is only used in print(Statement, ResultSet) because we're using
	//the defaultFormat (which applies to the whole table) and we have to rearrange the format
	//if columns are out of order
	private static String[] getAbsoluteColumnNames(ResultSet allColumnQuery) throws SQLException { //will get the column names in the Table
		ResultSetMetaData metadata = allColumnQuery.getMetaData();
		int columnCount = metadata.getColumnCount();
		String[] names = new String[columnCount];
		for (int i = 0; i < columnCount; i++) {
			names[i] = metadata.getColumnName(i+1);
		}
		return names;
	}
	
	private static int getMaxiumumColumnIndex(ResultSet allColumnQuery, String[] localColumnNames) throws SQLException {
		//Returns the index of the farthest right (max) column in the table. This index starts from 1.
		int len = localColumnNames.length;
		int[] indicies = new int[len];
		for (int i = 0; i < indicies.length; i++) {
			indicies[i] = allColumnQuery.findColumn(localColumnNames[i]);
		}
		Arrays.sort(indicies);
		return indicies[len-1];
	}
	
	//need to fix so that it doesn't rely on the columns being consecutive/sequential 
	//Bind all table column names to their approp. pattern. 
	//absoluteColumnNames should be the intrinsic order of the columns in the table. absoluteColumnNames and patterns are parallel arrays
	private static HashMap<String,String> bindColumnNameToFormatPattern(String[] absoluteColumnNames, String[] patterns, int maxIndex) throws SQLException {
		HashMap<String, String> namesToPattern = new HashMap<String, String>(maxIndex);
		for (int i = 0; i < maxIndex; i++) {
			namesToPattern.put(absoluteColumnNames[i], patterns[i]);
		}
		return namesToPattern;
	}
	
	//Returns a String[] of row values, uses the columnNames of the ResultSet as arguments 
	//to the ResultSet's getString() method.
	//Currently formats doubles to a USD currency format
	private static String[] getRowValues(ResultSet rs, String[] localColumnNames) throws SQLException {
		ResultSetMetaData metadata = rs.getMetaData();
		int columnCount = localColumnNames.length;
		String[] values = new String[columnCount];
		for (int i = 0; i < columnCount; i++) {
			String columnName = localColumnNames[i];
			if (metadata.getColumnType(i+1) == java.sql.Types.DOUBLE) {
				NumberFormat nf = NumberFormat.getCurrencyInstance();
				values[i] = nf.format(rs.getDouble(columnName));
			}
			else {
				values[i] = rs.getString(columnName);
			}
		}
		return values;
	}
	
	//The header will be a rearranged version of the original if anything has changed. At the end of the pattern "\n" will be added
	//a names array of the column names in the ResultSet retaining their order should be used
	private static String getRearrangedFormattedHeader(String[] localColumnNames, HashMap<String,String> nmToPat) throws SQLException {
		String newPattern = "";
		for (String name : localColumnNames) {
			newPattern += nmToPat.get(name);
		}
		newPattern += "\n";
		return newPattern;
	}
	
	//If the format passed in is < the number of columns, than we stretch it so we
	//match the column count and we won't have problems with printf. 
	private static String stretchPattern(int localColumnCount, String format) {
		String[] formatSpecifiers = getFormatSpecifiers(format);
		int formatSpecifierCount = formatSpecifiers.length;
		int quotient = localColumnCount/formatSpecifierCount; //number of times the format divides evenly into the columnCount
		if(quotient <= 1) { //should be equal to one IDEALLY, but if somehow they pass in a format > columns, should add case for that
			return format;
		}
		final StringBuilder pattern = new StringBuilder(); //StringBuilder is more efficient since only one object instance is created (is mutable) unlike strings
		int remainder = localColumnCount % formatSpecifierCount; //determines if we need to stretch the format because the format has less specifiers than there are columns														 
		for (int i = 0; i < quotient; i++) {
			pattern.append(format);
		}
		for (int i = 0; i < remainder; i++) {
			pattern.append(formatSpecifiers[i]);
		}
		return pattern.toString();
	}
	
	//Takes a string of format specifiers "%20s%20s" and produces ["%20s","%20s"]
	private static String[] getFormatSpecifiers(String format) {
		String regEx_SplitByAndIncludeDelimeter = "((?<=%)|(?=%))";
		String[] elements = format.split(regEx_SplitByAndIncludeDelimeter);
		//Format specifiers are like this "%20s"; the split will produce [%,-15s,%,12s,%,15s ,%,-40s,%,13s,%,17s] so 
		//the count of the specifiers is doubled, then we need to divide by 2 to get the true count
		int formatSpecifierCount = elements.length/2;
		String[] specifiers = new String[formatSpecifierCount];
		for (int i=0; i<formatSpecifierCount; i++) {
			specifiers[i] = elements[i*2] + elements[i*2+1]; //concatenate a pair of elements and insert into the new array
		}
		return specifiers; 
	}

	//Debug function. Prints to the console the state of the ResultSet:
	//current row, closed, after last row, on last row 
	private static void getResultSetStatus(ResultSet results) throws SQLException {
		System.out.println("ROW CURSOR ON?: " + results.getRow());
		System.out.println("CURSOR CLOSED?: " + results.isClosed());
		System.out.println("CURSOR AFTER LAST ROW?: " + results.isAfterLast());
		System.out.println("CURSOR ON LAST ROW?: " + results.isLast()); 
	}
	
}

