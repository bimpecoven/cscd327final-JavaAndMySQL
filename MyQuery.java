/*	Blake Impecoven - CSCD327 - Fall 2016
 * 
 * Project assignment for Databases
 *  
**/

import java.io.*;
import java.util.*;
import java.lang.String;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.CallableStatement;

public class MyQuery {

   	private Connection conn = null;
	private Statement statement = null;
	private ResultSet resultSet = null;
    
    public MyQuery(Connection c)throws SQLException {
        conn = c;
        statement = conn.createStatement();
    }//end MyQuery
    
	 //Query 0: this is a sample query provided by Dr. Li
	public void findChargeDate() throws SQLException {
        String query  = "SELECT crime_id, date_charged " +
                        "FROM crimes " + 
                        "WHERE date_charged <= \'2008-10-22\';";

        resultSet = statement.executeQuery(query);
    }//end findChargeDate
    
    public void printChargeDate() throws IOException, SQLException {
		System.out.println("******** Query 0 ********");
		System.out.println("Crime_ID\t" + "Charge_Date");
 
        while (resultSet.next()) {
			int crime_id = resultSet.getInt(1);
			Date charge_date = resultSet.getDate("date_charged");
			System.out.println(crime_id + "\t\t" + charge_date);
		}//end while        
    }//end printChargeDate

	    
   public void printBestOfficer() throws IOException, SQLException {
      	System.out.println("******** Query 1 ********");
		System.out.println( "Last, First, count" );
     	while( resultSet.next() ) {
         	String result = resultSet.getString("Last") + ", " + 
                     		resultSet.getString("First") + ", " + 
                     		resultSet.getInt("cnt");
         	System.out.println( result );
      	}//end while
	}//end printBestOfficer

	//Query 2
   	public void findCrimeCharge() throws SQLException {
      	String query = "SELECT charge_id " +
                       "FROM crime_charges " +
                       "WHERE fine_amount > (SELECT avg(fine_amount) FROM crime_charges) " +
                       "and amount_paid < (SELECT avg(amount_paid) FROM crime_charges);";
                        
      	resultSet = statement.executeQuery(query);
   	}//end findCrimeCharge

   	public void printCrimeCharge() throws IOException, SQLException {
		System.out.println("******** Query 2 ********");
      	System.out.println("Charge_ID");
      
      	while (resultSet.next()) {
		//fill in this portion
      
         	String result = resultSet.getString("charge_id");
         	System.out.println(result);
      	}//end while
    }//end printCrimeChange

    //Query 1
   	public void findBestOfficer() throws SQLException {
      	String queryOne = "SELECT first, last, count( crime_id ) cnt " +
                          "FROM officers join crime_officers using(officer_id) " +
                          "GROUP BY officers.officer_id " +
                          "HAVING count(crime_id)>(SELECT avg(cnt) " +
                          "FROM(SELECT count(crime_id) cnt " + 
                          "FROM crime_officers GROUP BY officer_id) temp );";

      	resultSet = statement.executeQuery(queryOne);                                                 
	}//end findBestOfficer
	
    //Query 4
    public void findCriminalSentence() throws SQLException {
       	String query = "SELECT criminal_id, last, first, count(sentence_id) cnt_sentence " +
                       "FROM criminals join sentences using(criminal_id) " +
                       "GROUP BY criminal_id " +
                       "HAVING count(sentence_id) > 1;";
                        
       	resultSet = statement.executeQuery(query); 
    }//end findCriminalSentence

    public void printCriminalSentence() throws IOException, SQLException {
		System.out.println("******** Query 4 ********");
       	System.out.println("Criminal_ID, Last,   First, Count_Sentence");
      
       	while (resultSet.next()) {
		//fill in this portion
      
          	String result = resultSet.getString("criminal_id") + ",        " + resultSet.getString("last") + ", " +
                          	resultSet.getString("first") + ",   " + resultSet.getString("cnt_sentence");
          	System.out.println(result);
       	}//end while       
    }//end printCriminalSentence

	//Query 5
    public void findChargeCount() throws SQLException
    {
	    String query = "SELECT precinct, count(charge_id) charge_cnt " +
                       "FROM officers join crime_officers using(officer_id) join crime_charges using(crime_id) " +
                       "WHERE charge_status = 'GL' " +
                       "GROUP BY precinct " +
                       "HAVING count(charge_id) >= 7; ";
                        
       	resultSet = statement.executeQuery(query);	
    }//end findChargeCount

    public void printChargeCount() throws IOException, SQLException {
		System.out.println("******** Query 5 ********");
       	System.out.println("Precinct, Charge_Count");
      
       	while (resultSet.next()) {
		//fill in this portion
      
         	String result = resultSet.getString("precinct") + ",     " + resultSet.getString("charge_cnt");
          	System.out.println(result);
       	}//end while       
    }//end printChargeCount
	 
     //Query 3
    public void findCriminal() throws SQLException {
      	String query = "SELECT distinct first, last " +
                       "FROM criminals join crimes using(criminal_id) " +
                       "WHERE criminal_id in (SELECT criminal_id FROM crimes join crime_charges using(crime_id) " +
                       "WHERE crime_code in (SELECT crime_code FROM crime_charges WHERE crime_id = 10089));";
                        
      	resultSet = statement.executeQuery(query);
    }//end findCriminal

    public void printCriminal() throws IOException, SQLException {
		System.out.println("******** Query 3 ********");
      	System.out.println("FIRST, LAST");
      
      	while (resultSet.next()) {
		//fill in this portion
      
         	String result = resultSet.getString("first") + ", " + resultSet.getString("last");
         	System.out.println(result);
      	}//end while
    }//end printCriminal

	//Query 6
	public void findEarliestLatest() throws SQLException {
	   	String query = "SELECT criminal_id, first, last, min(start_date) as 'earliest_start_date', max(end_date) as 'latest_end_date' " +
                       "FROM criminals join sentences using(criminal_id) " +
                       "GROUP BY criminals.criminal_id;";
      	resultSet = statement.executeQuery(query);   
   	}//end findEarliestLatest

   	public void printEarliestLatest() throws IOException, SQLException {
	   	System.out.println("******** Query 6 ********");
      	System.out.println( "Criminal_ID, First, Last, Earliest_Start_Date, Latest_End_Date" );
      	while( resultSet.next() ) {
         	String result = resultSet.getInt( "criminal_id" ) + ",        " + 
         					resultSet.getString("first" ) + ", " + 
         					resultSet.getString( "last" ) + ", " + 
                         	resultSet.getDate( "earliest_start_date" ) + ",        " + 
                         	resultSet.getDate( "latest_end_date" );
         	System.out.println(result); 
      	}//end while
   	}//end printEarliestLatest

	//Query 7
   	public void findCrimeCounts() throws SQLException {
	  	System.out.println("******** Query 7 ********");	
	   	InputStreamReader istream = new InputStreamReader(System.in) ;
      	BufferedReader bufRead = new BufferedReader(istream) ;	
      	boolean continueInput = true;   
          
	   	do {
         	try {
            	System.out.println("Please enter the officer_id for the query: ");
            	//fill in this portion
            
            	int id = 0;
            	int cnt = 0; 
            	id = Integer.parseInt(bufRead.readLine());
            
            	if((double)(id) / 1000000 > 1 || id < 0) { 
            		throw new InputMismatchException(); 
            	}//end if
            
            	continueInput = false;
            
            	String query = "SELECT count(charge_id) charge_cnt " +
                           	   "FROM crime_charges join crime_officers using(crime_id) " +
                               "GROUP BY officer_id " +
                               "HAVING officer_id = " + id + ";";                           
                        
            	resultSet = statement.executeQuery(query);
            
            	while (resultSet.next()) {
               		cnt = resultSet.getInt("charge_cnt");
			
	            	System.out.println("Officer " + id + " has reported " + cnt + " crimes.");
            	}//end while
	      	}//end try
         
         	catch (IOException err) {
            	System.out.println("Error reading line");
         	}//end catch
         	catch (InputMismatchException ex) {
            	System.out.println("Invalid officer_id");
        	}//end catch  
      	} while(continueInput);
    }//end findCrimeCounts
}//end class