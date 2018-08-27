import java.sql.*;
import java.util.*;
import java.text.Format.*;
  
  
public class SqlStatements {
	//DB stuff. Pretty self-explanatory. 
     static public String username = "F16336jsettine";
     static public String password = "23166497";
     static public String dbname = "F16336team4";
     static public final String url =  "jdbc:mysql://134.74.126.107:3306/" + dbname;
    public SqlStatements(String username, String dbname, String password)
    {

	this.username = username;
	this.password = password;
	this.dbname = dbname; 
    }
    public static void main(String args[]) {
	// Main items for querying. We set the author1 and author2, and then extracted the data properly.
	//For these authors, the pub_id is shared by one of them, and the stor_id array is shared by all of them. 
	//The other information is dummy info. 
	

        String title_id = "123456";
        String ord_num = "'AK-1111'";
        String[] stor_id = {"0736", "1389", "5023", "9347", "9333", "9012", "9678", "9283", "8042", "7131", "7066"};
        String pub_id = "'1389'"; //This pub_id belongs to au_id 998-72-3567. 
        String author1 = "'172-32-1176'"; 
        String author2 = "'998-72-3567'";
     
         java.util.Date javaDate = new java.util.Date(); //for rev_date. 
         long javaTime = javaDate.getTime();
         java.sql.Timestamp sqlTimestamp = new java.sql.Timestamp(javaTime); //for date in sales
         java.sql.Date sqlDate = new java.sql.Date(javaTime);
	//most of the sql statements used in the project
     		     String[] sqlstat = { "INSERT INTO  titles(title_id, title, type, pub_id, price, advance, total_sales, notes, pubdate, contract, ag_id) VALUES ( " + title_id + ", 'Catcher in the Rye', 'fiction'," + pub_id + ", 19.99, 500.00, 650, 'foo', NULL, 0, NULL);",
                    "INSERT INTO  titleauthor(au_id, title_id, au_ord, royaltyper) VALUES (" + author1 + "," + title_id + ", 1, NULL);",
                    "INSERT INTO  titleauthor(au_id, title_id, au_ord, royaltyper) VALUES (" + author2 + "," + title_id + ", 2, NULL);",
                    "INSERT INTO sales(stor_id, ord_num, date) VALUES (?, ?, ?);", 
                    "SELECT * FROM pending_orders WHERE title_id = " + title_id + ";",
                    "UPDATE pending_orders SET fulfilled = 1 WHERE title_id = '123456';",
                    "UPDATE store_inventories s INNER JOIN pending_orders p ON s.title_id = p.title_id SET s.qty = (s.qty - p.qty)",
                    "INSERT INTO reviews(review_id, title_id, periodical_id, rev_date, rating, content) VALUES(?, ?, ?, ?, ?, ?);"};
      
      if(IsSQLWorking(username, password, dbname)) 
      {
        try {

           	Class.forName("com.mysql.jdbc.Driver");
           	Connection myconnect = DriverManager.getConnection(url, username, password);
	        Statement st = myconnect.createStatement();

		/* Here the authors "publish" a "new" book -- Catcher in the Rye. 
		   They are inserted into the titles table, 
		   and then inserted as authors of the book into the titleauthor table 
		*/
                st.executeUpdate(sqlstat[0]);
                st.executeUpdate(sqlstat[1]);
                st.executeUpdate(sqlstat[2]);


		//This part inserts into the sales, sales_detail, and the pending_orders table all of the stores that the authors have in common
               for(int i = 0; i < stor_id.length; i++)
	       {
	       	 PreparedStatement ps1 = myconnect.prepareStatement(sqlstat[3]);
		 ps1.setString(1, stor_id[i]);
		 ps1.setString(2, "AK-1111");
		 ps1.setTimestamp(3, sqlTimestamp);
		 ps1.executeUpdate();
		 PreparedStatement ps3 = myconnect.prepareStatement("INSERT INTO salesdetail(stor_id, ord_num, title_id, qty, discount) VALUES (?, ?, ?, ?, ?);");
		 ps3.setString(1, stor_id[i]);
		 ps3.setString(2, "AK-1111");
		 ps3.setString(3, title_id);
		 ps3.setInt(4, 100);
		 ps3.setInt(5, 40);
		 ps3.executeUpdate();
		 PreparedStatement ps4 = myconnect.prepareStatement("INSERT INTO pending_orders(stor_id, ord_num, title_id, qty, fulfilled) VALUES (?, ?, ?, ?, ?);");
		 ps4.setString(1, stor_id[i]);
		 ps4.setString(2, "AK-1111");
		 ps4.setString(3, title_id);
		 ps4.setInt(4, 100);
		 ps4.setInt(5, 0);
		 ps4.executeUpdate();
		}


		// This part gets the pending orders as well as the sales and salesdetail data that we have parsed 
	       ResultSet rs = st.executeQuery(sqlstat[4]);
               System.out.println("Fetching the data from pending_orders:");
               while(rs.next())
               {
                  System.out.println(rs.getString("stor_id") + "\t" + rs.getString("title_id") + "\t" + rs.getString("fulfilled")); 
               }
	       ResultSet sales = st.executeQuery("SELECT distinct salesdetail.stor_id, salesdetail.ord_num, salesdetail.title_id, salesdetail.qty, salesdetail.discount FROM salesdetail JOIN sales ON (salesdetail.stor_id = sales.stor_id) and (salesdetail.title_id =" + title_id + ");");
	       System.out.println("Generating the sales and salesdetail records");
 	       while(sales.next())
	           System.out.println(sales.getString("stor_id") + "\t" + sales.getString("ord_num") + "\t" + sales.getString("title_id"));


               //This part updates the pending orders to fulfilled, as well as the bookstore inventories
               st.executeUpdate(sqlstat[5]);
               st.executeUpdate(sqlstat[6]);


		//Obtain/get review that is published. 
		PreparedStatement ps2 = myconnect.prepareStatement(sqlstat[7]);
		ps2.setString(1, "1234");
		ps2.setString(2, title_id);
		ps2.setString(3, "INT000");
		ps2.setDate(4, sqlDate);
		ps2.setInt(5, 1);
		ps2.setString(6, "Disturbing, but amazing book is Catcher in the Rye");
		ps2.executeUpdate();
		
               
       
	          
       }
	//if problems(obviously)
       catch (Exception ex){
        ex.printStackTrace();
       }


      }

    }

    public static boolean IsSQLWorking(String username, String password, String dbname)
	{
        try {
                Class.forName("com.mysql.jdbc.Driver");
		String url = "jdbc:mysql://134.74.126.107:3306/" + dbname;
		Connection cn1 = DriverManager.getConnection(url, username, password);
		return true; 
		
        }
        catch (Exception ex) {
            return false;
        }
     }
    
}