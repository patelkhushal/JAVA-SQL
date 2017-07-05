/*
 * Name: Khushal Patel
 * EEECS 3421 Project 2
 * Student id: 214037618
 */

import java.util.*;
import java.text.*;
import java.sql.*;

public class SimDb2 {
	private Connection conDB; // Connection to the database system.
	private String url; // URL: Which database?

	private Integer custID; // Who are we tallying?
	private HashMap<Integer, String> books = new HashMap<Integer, String>();
	private HashMap<Integer, String[]> map = new HashMap<Integer, String[]>();
	private int bCount;
	private int totalBooks;
	private String club;

	// Constructor
	public SimDb2() throws NumberFormatException, ParseException {
		// Set up the DB connection.
		try {
			// Register the driver with DriverManager.
			Class.forName("com.ibm.db2.jcc.DB2Driver").newInstance();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(0);
		} catch (InstantiationException e) {
			e.printStackTrace();
			System.exit(0);
		} catch (IllegalAccessException e) {
			e.printStackTrace();
			System.exit(0);
		}

		// URL: Which database?
		url = "jdbc:db2:c3421m";

		// Initialize the connection.
		try {
			// Connect with a fall-thru id & password
			conDB = DriverManager.getConnection(url);
		} catch (SQLException e) {
			System.out.print("\nSQL: database connection error.\n");
			System.out.println(e.toString());
			System.exit(0);
		}

		System.out.print("Enter CustID: ");
		@SuppressWarnings("resource")
		Scanner input = new Scanner(System.in);
		String custID = input.nextLine();

		boolean validIn = false;
		while (!validIn) {
			if (!isInteger(custID)) {
				System.out.print("Please enter a number: ");
				custID = input.nextLine();
				validIn = false;
			} else if (!find_customer(custID)) {
				System.out.print("Please enter a valid CustID: ");
				custID = input.nextLine();
				validIn = false;
			} else {
				validIn = true;
			}
		}
		boolean check = false;
		String bTitle = "";
		String cat = "";
		String cap = "";
		while(!check)
		{
			System.out.println("\nSelect a category from the following:");
			fetch_categories();

			cat = cat_check();
			cap = cat.substring(0, 1).toUpperCase() + cat.substring(1);
			System.out.println("Following are the available books for the category \"" + cap + "\": \n");
			fetch_books(cat);
			
			System.out.print("\nEnter the book# or enter zero(0) to change the category: ");
			bTitle = input.nextLine();
			
			validIn = false;
			while (!validIn) {
				if (!isInteger(bTitle)) {
					System.out.print("Please enter a number: ");
					bTitle = input.nextLine();
					validIn = false;
				} else if (Integer.parseInt(bTitle) < 0 || Integer.parseInt(bTitle) > bCount) {
					System.out.print("Please enter a number between 0 and " + bCount + ": ");
					bTitle = input.nextLine();
					validIn = false;
				} else {
					validIn = true;
				}
			}
			if(bTitle.equals("0"))
			{
				check = false;
			}
			else
			{
				check = true;
			}
			
		}
		
		
			int num = Integer.parseInt(bTitle);
			String title = books.get(num);
			validIn = fetch_book(title, cat);
			
			System.out.print("Enter the book#: ");
			String userBook = input.nextLine();
			int key = 0;
			validIn = false;
			while(!validIn)
			{
				if(!isInteger(userBook))
				{
					validIn = false;
					System.out.print("Please enter a number: ");
					userBook = input.nextLine();
				}
				else if(Integer.parseInt(userBook) < 1 || Integer.parseInt(userBook) > totalBooks)
				{
					validIn = false;
					if(totalBooks > 1)
					{
						System.out.print("Please enter a valid number [1-" + totalBooks + "]: ");
						userBook = input.nextLine();
					}
					else
					{
						System.out.print("Only one book available. Please enter 1 to proceed: ");
						userBook = input.nextLine();
					}
					
				}
				else
				{
					validIn = true;
					key = Integer.parseInt(userBook);
				}
			}
			
			String[] arr = map.get(key);
			
			double price = fetch_price(arr);
			DecimalFormat df = new DecimalFormat("#.##");
			System.out.println("The minimum price for the selected book is " + price);
			System.out.print("Enter the number of books (the quantity) to buy: ");
			String userQt = input.nextLine();
			
			validIn = false;
			while (!validIn) 
			{
				if (!isInteger(userQt))
				{
					System.out.print("Please enter a number: ");
					userQt = input.nextLine();
					validIn = false;
				} 
				else if(Integer.parseInt(userQt) <= 0)
				{
					System.out.print("Please enter a number greater than zero: ");
					userQt = input.nextLine();
					validIn = false;
				}
				else 
				{
					validIn = true;
				}
			}
			
			club = fetch_club(price, arr);
			double total = price * Integer.parseInt(userQt);
			System.out.println("Total Price: " + df.format(total));
			System.out.print("Would you like to purchase the book/books? [Y/N]: ");
			String confirm = input.nextLine();
			
			
			validIn = false;
			while(!validIn)
			{
				if(confirm.equals("Y") || confirm.equals("y") || confirm.equals("N") || confirm.equals("n"))
				{
					validIn = true;
				}
				else
				{
					validIn = false;
					System.out.print("Please enter either y or Y for Yes or n or N for No: ");
					confirm = input.nextLine();
				}
			}
			
			if(confirm.equals("Y") || confirm.equals("y"))
			{
				insertIntoPurchase(arr[0], Integer.parseInt(arr[1]), Integer.parseInt(userQt));
				System.out.println("Order placed. Thank You for buying from us.");
			}
			else
			{
				System.out.println("No books purchased. Goodbye!");
			}	
		
		
		//input.close();
			

		// Let's have autocommit turned off. No particular reason here.
		try {
			conDB.setAutoCommit(false);
		} catch (SQLException e) {
			System.out.print("\nFailed trying to turn autocommit off.\n");
			e.printStackTrace();
			System.exit(0);
		}

		// Commit. Okay, here nothing to commit really, but why not...
		try {
			conDB.commit();
		} catch (SQLException e) {
			System.out.print("\nFailed trying to commit.\n");
			e.printStackTrace();
			System.exit(0);
		}
		// Close the connection.
		try {
			conDB.close();
		} catch (SQLException e) {
			System.out.print("\nFailed trying to close the connection.\n");
			e.printStackTrace();
			System.exit(0);
		}

	}
	
	private void fetch_books(String cat)
	{
		String queryText = ""; // The SQL text.
		PreparedStatement querySt = null; // The query handle.
		ResultSet answers = null; // A cursor.
		
		queryText = "select distinct title from yrb_book where cat = ? and title in (select o1.title from yrb_offer o1 where o1.club in (select club from yrb_member where cid = ?)) and year in (select o1.year from yrb_offer o1 where o1.club in (select club from yrb_member where cid = ?))";
	


		// Prepare the query.
		try {			
			querySt = conDB.prepareStatement(queryText);
		} catch (SQLException e) {
			System.out.println("SQL#1 failed in prepare");
			System.out.println(e.toString());
			System.exit(0);
		}

		// Execute the query.
		try {
			querySt.setString(1, cat);
			querySt.setInt(2, custID);
			querySt.setInt(3, custID);
			answers = querySt.executeQuery();
		} catch (SQLException e) {
			System.out.println("SQL#1 failed in execute");
			System.out.println(e.toString());
			System.exit(0);
		}

		// Any answer?
		try {
			boolean next = answers.next();
			int count = 1;
			String s;
			for (; next; count++) 
			{			
				s = answers.getString(1);
				System.out.println(count + ". " + s);
				books.put(count, s);
				if(answers.next() == true)
				{
					next = true;
				}
				else
				{
					next = false;
				}
			}
			bCount = count - 1;
		} catch (SQLException e) {
			System.out.println("SQL#1 failed in cursor.");
			System.out.println(e.toString());
			System.exit(0);
		}

		// Close the cursor.
		try {
			answers.close();
		} catch (SQLException e) {
			System.out.print("SQL#1 failed closing cursor.\n");
			System.out.println(e.toString());
			System.exit(0);
		}

		// We're done with the handle.
		try {
			querySt.close();
		} catch (SQLException e) {
			System.out.print("SQL#1 failed closing the handle.\n");
			System.out.println(e.toString());
			System.exit(0);
		}

	}

	private void insertIntoPurchase(String title, int year, int qnty) throws ParseException
	{
		String queryText = ""; // The SQL text.
		PreparedStatement querySt = null; // The query handle.
		
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());

	    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH.mm.ss");
	    String text = dateFormat.format(timestamp);
		
		queryText = "INSERT INTO yrb_purchase VALUES (?,?,?,?,?,?)";
		//db2 "select o1.club from yrb_member m1, yrb_offer o1 where o1.club = m1.club and o1.year = 1997 and m1.cid = 4 and o1.title = 'Richmond Underground' and price = 14.95"


		// Prepare the query.
		try {			
			querySt = conDB.prepareStatement(queryText);
		} catch (SQLException e) {
			System.out.println("SQL#1 failed in prepare");
			System.out.println(e.toString());
			System.exit(0);
		}

		// Execute the query.
		try {
			
			querySt.setInt(1, custID);
			querySt.setString(2, club);
			querySt.setString(3, title);
			querySt.setInt(4, year);
			querySt.setString(5, text);
			querySt.setInt(6, qnty);
			
			querySt.executeUpdate();
		} catch (SQLException e) {
			System.out.println("SQL#1 failed in update");
			System.out.println(e.toString());
			System.exit(0);
		}
		
		// We're done with the handle.
		try {
			querySt.close();
		} catch (SQLException e) {
			System.out.print("SQL#1 failed closing the handle.\n");
			System.out.println(e.toString());
			System.exit(0);
		}

	}
	
	private String fetch_club(double price, String[] arr)
	{
		String title = arr[0];
		String year = arr[1];
		int yearInt = Integer.parseInt(year);
		String club = "";
		
		String queryText = ""; // The SQL text.
		PreparedStatement querySt = null; // The query handle.
		ResultSet answers = null; // A cursor.
		
		queryText = "select o1.club from yrb_member m1, yrb_offer o1 where o1.club = m1.club and o1.year = ? and m1.cid = ? and o1.title = ? and o1.price = ?";
		//db2 "select o1.club from yrb_member m1, yrb_offer o1 where o1.club = m1.club and o1.year = 1997 and m1.cid = 4 and o1.title = 'Richmond Underground' and price = 14.95"


		// Prepare the query.
		try {			
			querySt = conDB.prepareStatement(queryText);
		} catch (SQLException e) {
			System.out.println("SQL#1 failed in prepare");
			System.out.println(e.toString());
			System.exit(0);
		}

		// Execute the query.
		try {
			
			querySt.setInt(1, yearInt);
			querySt.setInt(2, custID);
			querySt.setString(3, title);
			querySt.setDouble(4, price);
			answers = querySt.executeQuery();
		} catch (SQLException e) {
			System.out.println("SQL#1 failed in execute");
			System.out.println(e.toString());
			System.exit(0);
		}

		// Any answer?
		try {
			if(answers.next())
			{
				club = answers.getString(1);
			}
			else
			{
				System.out.println("No club found");
			}
		} catch (SQLException e) {
			System.out.println("SQL#1 failed in cursor.");
			System.out.println(e.toString());
			System.exit(0);
		}

		// Close the cursor.
		try {
			answers.close();
		} catch (SQLException e) {
			System.out.print("SQL#1 failed closing cursor.\n");
			System.out.println(e.toString());
			System.exit(0);
		}

		// We're done with the handle.
		try {
			querySt.close();
		} catch (SQLException e) {
			System.out.print("SQL#1 failed closing the handle.\n");
			System.out.println(e.toString());
			System.exit(0);
		}

		
		return club;
	}
	
	private double fetch_price(String[] arr)
	{
		String queryText = ""; // The SQL text.
		PreparedStatement querySt = null; // The query handle.
		ResultSet answers = null; // A cursor.
		String userTitle = arr[0];
		String year = arr[1];
		double price = 0.0;

		queryText = "select distinct min(price) from yrb_offer where title = ? and year = ? and club in (select club from yrb_member where cid = ?)"; //and price <= ALL(select price from yrb_offer where title = ?)";
		//db2 "select distinct price from yrb_offer where title = 'Richmond Underground' and club in (select club from yrb_member where cid = 4) and price <= ALL(select price from yrb_offer where title = 'Richmind Underground')"


		// Prepare the query.
		try {			
			querySt = conDB.prepareStatement(queryText);
		} catch (SQLException e) {
			System.out.println("SQL#1 failed in prepare");
			System.out.println(e.toString());
			System.exit(0);
		}

		// Execute the query.
		try {
			querySt.setString(1, userTitle);
			//System.out.println(userTitle);
			querySt.setInt(2, Integer.parseInt(year));
			querySt.setInt(3, custID);
			answers = querySt.executeQuery();
		} catch (SQLException e) {
			System.out.println("SQL#1 failed in execute");
			System.out.println(e.toString());
			System.exit(0);
		}

		// Any answer?
		try {
			if(answers.next())
			{
				price = answers.getDouble(1);
			}
			else
			{
				System.out.println("No price value found");
			}
		} catch (SQLException e) {
			System.out.println("SQL#1 failed in cursor.");
			System.out.println(e.toString());
			System.exit(0);
		}

		// Close the cursor.
		try {
			answers.close();
		} catch (SQLException e) {
			System.out.print("SQL#1 failed closing cursor.\n");
			System.out.println(e.toString());
			System.exit(0);
		}

		// We're done with the handle.
		try {
			querySt.close();
		} catch (SQLException e) {
			System.out.print("SQL#1 failed closing the handle.\n");
			System.out.println(e.toString());
			System.exit(0);
		}

		
		return price;
	}
	
	
	
	public boolean fetch_book(String title, String cat)
	{
		String queryText = ""; // The SQL text.
		PreparedStatement querySt = null; // The query handle.
		ResultSet answers = null; // A cursor.

		boolean inDB = false; // Return.

		queryText = "SELECT distinct b.title, b.year, b.language, b.cat, b.weight " + "FROM yrb_book b, yrb_offer o " + "WHERE b.title = ?" + " AND cat = ? and b.title in (select o1.title from yrb_offer o1 where o1.club in (select club from yrb_member where cid = ?)) and b.year in (select o1.year from yrb_offer o1 where o1.club in (select club from yrb_member where cid = ?) and o1.title = ?)";
		
		
		String[] book = new String[4]; //for title, year, language, weight (in order)

		// Prepare the query.
		try {
			querySt = conDB.prepareStatement(queryText);
		} catch (SQLException e) {
			System.out.println("SQL#1 failed in prepare");
			System.out.println(e.toString());
			System.exit(0);
		}

		// Execute the query.
		try {
			querySt.setString(1, title);
			querySt.setString(2, cat);
			querySt.setInt(3, custID);
			querySt.setInt(4, custID);
			querySt.setString(5, title);
			answers = querySt.executeQuery();
		} catch (SQLException e) {
			System.out.println("SQL#1 failed in execute");
			System.out.println(e.toString());
			System.exit(0);
		}

		// Any answer?
		try {
			if(!answers.next())
			{
				inDB = false;
			}
			else
			{
				System.out.println("\nChoose a book to buy from the following available book(s):");
				inDB = true;
				boolean next = true;
				int count = 1;
				for (; next; count++) 
				{
					book = new String[4];
					book[0] = answers.getString(1);
					book[1] = String.valueOf(answers.getInt(2));
					book[2] = answers.getString(3);
					book[3] = String.valueOf(answers.getInt(5));
					map.put(count, book);
					
					
					System.out.println("\nBook " + count + ":");
					System.out.println("Title: " + answers.getString(1));
					System.out.println("Year: " + answers.getInt(2));
					System.out.println("Language: " + answers.getString(3));
					System.out.println("Weight: " + answers.getInt(5));
					if(answers.next() == true)
					{
						next = true;
					}
					else
					{
						next = false;
					}
				}
				
				totalBooks = count - 1;
			}
			
			
		} catch (SQLException e) {
			System.out.println("SQL#1 failed in cursor.");
			System.out.println(e.toString());
			System.exit(0);
		}

		// Close the cursor.
		try {
			answers.close();
		} catch (SQLException e) {
			System.out.print("SQL#1 failed closing cursor.\n");
			System.out.println(e.toString());
			System.exit(0);
		}

		// We're done with the handle.
		try {
			querySt.close();
		} catch (SQLException e) {
			System.out.print("SQL#1 failed closing the handle.\n");
			System.out.println(e.toString());
			System.exit(0);
		}

		return inDB;
		
	}
	
	public String cat_check()
	{
		System.out.print("\nEnter category number [1-12]: ");
		@SuppressWarnings("resource")
		Scanner in = new Scanner(System.in);
		String catN = in.nextLine();
		String cat;
		boolean validCat = false;
		while (!validCat) 
		{
			if (!isInteger(catN)) 
			{
				System.out.print("Please enter a number between 1 and 12: ");
				catN = in.nextLine();
				validCat = false;
			}
			else if (Integer.parseInt(catN) < 1 || Integer.parseInt(catN) > 12) 
			{
				System.out.print("Please enter a number between 1 and 12: ");
				catN = in.nextLine();
				validCat = false;
			} 
			else 
			{
				validCat = true;
			}
		}

		switch (catN) {
		case "1":
			cat = "children";
			break;
		case "2":
			cat = "cooking";
			break;
		case "3":
			cat = "drama";
			break;
		case "4":
			cat = "guide";
			break;
		case "5":
			cat = "history";
			break;
		case "6":
			cat = "horror";
			break;
		case "7":
			cat = "humor";
			break;
		case "8":
			cat = "mystery";
			break;
		case "9":
			cat = "phil";
			break;
		case "10":
			cat = "romance";
			break;
		case "11":
			cat = "science";
			break;
		case "12":
			cat = "travel";
			break;
		default:
			cat = "Invalid value";
			break;
		}
		//in.close();
		return cat;
	}

	public void fetch_categories() {
		String queryText = ""; // The SQL text.
		PreparedStatement querySt = null; // The query handle.
		ResultSet answers = null; // A cursor.

		queryText = "SELECT *       " + "FROM yrb_category ";

		// Prepare the query.
		try {
			querySt = conDB.prepareStatement(queryText);
		} catch (SQLException e) {
			System.out.println("SQL#1 failed in prepare");
			System.out.println(e.toString());
			System.exit(0);
		}

		// Execute the query.
		try {
			answers = querySt.executeQuery();
		} catch (SQLException e) {
			System.out.println("SQL#1 failed in execute");
			System.out.println(e.toString());
			System.exit(0);
		}

		// Any answer?
		try {
			for (int i = 1; answers.next(); i++) {
				String str = answers.getString(1);
				String cap = str.substring(0, 1).toUpperCase() + str.substring(1);
				System.out.println(i + ". " + cap);
			}
		} catch (SQLException e) {
			System.out.println("SQL#1 failed in cursor.");
			System.out.println(e.toString());
			System.exit(0);
		}

		// Close the cursor.
		try {
			answers.close();
		} catch (SQLException e) {
			System.out.print("SQL#1 failed closing cursor.\n");
			System.out.println(e.toString());
			System.exit(0);
		}

		// We're done with the handle.
		try {
			querySt.close();
		} catch (SQLException e) {
			System.out.print("SQL#1 failed closing the handle.\n");
			System.out.println(e.toString());
			System.exit(0);
		}

	}

	public boolean find_customer(String id) {
		String queryText = ""; // The SQL text.
		PreparedStatement querySt = null; // The query handle.
		ResultSet answers = null; // A cursor.

		boolean inDB = false; // Return.

		queryText = "SELECT *       " + "FROM yrb_customer " + "WHERE cid = ?     ";

		// Prepare the query.
		try {
			querySt = conDB.prepareStatement(queryText);
		} catch (SQLException e) {
			System.out.println("SQL#1 failed in prepare");
			System.out.println(e.toString());
			System.exit(0);
		}

		// Execute the query.
		try {
			querySt.setInt(1, Integer.parseInt(id));
			answers = querySt.executeQuery();
		} catch (SQLException e) {
			System.out.println("SQL#1 failed in execute");
			System.out.println(e.toString());
			System.exit(0);
		}

		// Any answer?
		try {
			if (answers.next()) {
				inDB = true;
				custID = answers.getInt(1);
				System.out.println("\nCustomer Information: ");
				System.out.println("Cid: " + answers.getInt(1));
				System.out.println("Name: " + answers.getString(2));
				System.out.println("City: " + answers.getString(3));
			}
		} catch (SQLException e) {
			System.out.println("SQL#1 failed in cursor.");
			System.out.println(e.toString());
			System.exit(0);
		}

		// Close the cursor.
		try {
			answers.close();
		} catch (SQLException e) {
			System.out.print("SQL#1 failed closing cursor.\n");
			System.out.println(e.toString());
			System.exit(0);
		}

		// We're done with the handle.
		try {
			querySt.close();
		} catch (SQLException e) {
			System.out.print("SQL#1 failed closing the handle.\n");
			System.out.println(e.toString());
			System.exit(0);
		}

		return inDB;
	}

	public static boolean isInteger(String s) {
		try {
			Integer.parseInt(s);
		} catch (NumberFormatException e) {
			return false;
		} catch (NullPointerException e) {
			return false;
		}
		// only got here if we didn't return false
		return true;
	}

	public static void main(String[] args) throws NumberFormatException, ParseException {
		new SimDb2();
	}
}
