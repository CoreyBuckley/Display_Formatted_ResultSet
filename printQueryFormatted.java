/* Corey Buckley
 * 3/30/18
 * CISS 241-300, HVCC
 * Assignment 3 - Part 3
 * 
 */

package dbms20;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.HashMap;

public class Assignment3_Part3 {
	
	private static final String createString = "CREATE TABLE Comics (" + 
												  "ComicName varchar(255), " +
												  "IssueNumber int, " +
												  "IssueDate varchar(10), " +
												  "IssueName varchar(255), " +
												  "IssueValue double, " +
												  "MintCondition varchar(3)," +
												  "Test1 varchar(10))";
	
	private static final String[] partitionedHeaderFormat = {"%-15s","%12s",
															 "%15s ","%-40s",
															 "%13s","%17s"};
	
	public static void main(String[] args) {
		try {
			SimpleDataSource.init("databasePart3.properties"); // Database initialization
		} catch (Exception e) { }
		try (Connection con = SimpleDataSource.getConnection(); Statement stmt = con.createStatement()){
			createTableAndPopulate(stmt);
			ResultSet results = stmt.executeQuery("SELECT ComicName, IssueDate, IssueNumber, IssueValue FROM Comics WHERE IssueValue >= 5 ORDER BY IssueValue DESC");
			printQueryFormatted(con, results);
			ResultSet results2 = stmt.executeQuery("SELECT IssueName, ComicName, IssueNumber, IssueDate FROM Comics WHERE ComicName='Amazing SM' AND IssueNumber<200");
			System.out.println();
			printQueryFormatted(con, results2);
		} 
		catch (SQLException ex)
		// if can't execute SQL statement
		{
			System.out.println("SQLException");
			System.out.println(ex.getMessage());
			ex.printStackTrace();
		}
	}
	
	private static void createTableAndPopulate(Statement stmt) {
		try {
			stmt.executeUpdate(createString); //if the table already exists, an error will be thrown and we will ignore adding more values to the already existing table
			//populate the database with records
			stmt.executeUpdate("INSERT INTO Comics values('Amazing SM', 89, '10/1/70', 'Doc Ock Lives', 6.50, 'No','crap')");
			stmt.executeUpdate("INSERT INTO Comics values('Spectacular SM', 92, '7/1/84', 'What Is The Answer', 4.50, 'No','crap')");
			stmt.executeUpdate("INSERT INTO Comics values('Web Of SM', 35, '2/1/88', 'You Can Go Home Again', 6.50, 'No','crap')");
			stmt.executeUpdate("INSERT INTO Comics values('Amazing SM', 382, '10/1/93', 'Emerald Rage', 4.00, 'Yes','crap')");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	private static void printQueryFormatted(Connection con, ResultSet results) throws SQLException {
		long begin = System.nanoTime();
		ResultSetMetaData metadata = results.getMetaData();
		Statement stmt = con.createStatement(); //if use the same statement then the resultset gets closed
		ResultSet all = stmt.executeQuery("SELECT * FROM Comics");
		String headerFormat = "%-15s%12s%15s %-40s%13s%17s";
		final String[] localColumnNames = getLocalColumnNames(metadata);
		final String[] absoluteColumnNames = getAbsoluteColumnNames(all);
		final int maxColumnIndex = getMaxiumumColumnIndex(all, localColumnNames);
		String[] rowValues;
		HashMap<String,String> columnNameToFormatting = bindColumnNameToFormatPattern(absoluteColumnNames, partitionedHeaderFormat, maxColumnIndex); 
		headerFormat = getFormattedHeader(localColumnNames, columnNameToFormatting);
		System.out.printf(headerFormat, localColumnNames);	
		System.out.println(); //line between the header and the records
		while(results.next()) {
			rowValues = getRowValues(results, localColumnNames);
			System.out.printf(headerFormat, rowValues);
		} 
		stmt.close(); //releasing the resources of the statement also closes the ResultSet (according to doc)
		long end = System.nanoTime();
		long duration = (end-begin)/1000000; //divide by a million is milliseconds
		System.out.println("\n\n\n" + duration + "ms elapsed" );
	}
	
	private static String[] getLocalColumnNames(ResultSetMetaData metadata) throws SQLException { //will get the column names in the ResultSet
		int columnCount = metadata.getColumnCount();
		String[] columnNames = new String[columnCount];
		for (int i = 0; i < columnCount; i++) {
			columnNames[i] = metadata.getColumnName(i+1);
		}
		return columnNames;
	}
	
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
	
	private static HashMap<String,String> bindColumnNameToFormatPattern(String[] absoluteColumnNames, String[] patterns, int maxIndex) throws SQLException {
		//Bind the column names to their approp. pattern. names[] should be the intrinsic order of the columns in the table. names and patterns are parallel arrays
		HashMap<String, String> namesToPattern = new HashMap<String, String>(maxIndex);
		for (int i = 0; i < maxIndex; i++) {
			namesToPattern.put(absoluteColumnNames[i], patterns[i]);
		}
		return namesToPattern;
	}
	
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
	
	private static String getFormattedHeader(String[] localColumnNames, HashMap<String,String> nmToPat) throws SQLException {
		//a names array of the column names in the ResultSet retaining their order should be used
		//The header will be a rearranged version of the original if anything has changed. At the end of the pattern "\n\n" will be added
		String newPattern = "";
		for (String name : localColumnNames) {
			newPattern += nmToPat.get(name);
		}
		newPattern += "\n";
		return newPattern;
	}

}
