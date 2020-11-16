package osu.cse3241;

import java.sql.*;
import java.util.Scanner;

/**
 * <h1>CSE3241 Introduction to Database Systems - Sample Java application.</h1>
 * 
 * <p>Sample app to be used as guidance and a foundation for students of 
 * CSE3241 Introduction to Database Systems at 
 * The Ohio State University.</p>
 * 
 * <h2>!!! - Vulnerable to SQL injection - !!!</h2>
 * <p>Correct the code so that it is not vulnerable to a SQL injection attack. ("Parameter substitution" is the usual way to do this.)</p>
 * 
 * <p>Class is written in Java SE 8 and in a procedural style. Implement a constructor if you build this app out in OOP style.</p>
 * <p>Modify and extend this app as necessary for your project.</p>
 *
 * <h2>Language Documentation:</h2>
 * <ul>
 * <li><a href="https://docs.oracle.com/javase/8/docs/">Java SE 8</a></li>
 * <li><a href="https://docs.oracle.com/javase/8/docs/api/">Java SE 8 API</a></li>
 * <li><a href="https://docs.oracle.com/javase/8/docs/technotes/guides/jdbc/">Java JDBC API</a></li>
 * <li><a href="https://www.sqlite.org/docs.html">SQLite</a></li>
 * <li><a href="http://www.sqlitetutorial.net/sqlite-java/">SQLite Java Tutorial</a></li>
 * </ul>
 *
 * <h2>MITx License</h2>
 *
 * <em>Copyright (c) 2019 Leon J. Madrid, Jeff Hachtel</em>
 * 
 * <p>Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.</p>
 *
 * 
 * @author Leon J. Madrid (madrid.1), Jeff Hachtel (hachtel.5)
 * 
 */

public class CSE3241app {
    
	/**
	 *  The database file name.
	 *  
	 *  Make sure the database file is in the root folder of the project if you only provide the name and extension.
	 *  
	 *  Otherwise, you will need to provide an absolute path from your C: drive or a relative path from the folder this class is in.
	 */
	private static String DATABASE = "librarynewdb.db";

	private static final int MAX_AUDIOBOOK_ID = 999;
	private static final int MAX_MOVIE_ID = 1999;
	private static final int MAX_GAME_ID = 2999;
	private static final int MAX_ALBUM_ID = 3999;

	private static final int MIN_AUDIOBOOK_ID = 0;
	private static final int MIN_MOVIE_ID = 1000;
	private static final int MIN_GAME_ID = 2000;
	private static final int MIN_ALBUM_ID = 3000;

    /**
     * Connects to the database if it exists, creates it if it does not, and returns the connection object.
     * 
     * @param databaseFileName the database file name
     * @return a connection object to the designated database
     */
    public static Connection initializeDB(String databaseFileName) {
    	/**
    	 * The "Connection String" or "Connection URL".
    	 * 
    	 * "jdbc:sqlite:" is the "subprotocol".
    	 * (If this were a SQL Server database it would be "jdbc:sqlserver:".)
    	 */
        String url = "jdbc:sqlite:" + databaseFileName;
        Connection conn = null; // If you create this variable inside the Try block it will be out of scope
        try {
            conn = DriverManager.getConnection(url);
            if (conn != null) {
            	// Provides some positive assurance the connection and/or creation was successful.
                DatabaseMetaData meta = conn.getMetaData();
                System.out.println("The driver name is " + meta.getDriverName());
                System.out.println("The connection to the database was successful.");
            } else {
            	// Provides some feedback in case the connection failed but did not throw an exception.
            	System.out.println("Null Connection");
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            System.out.println("There was a problem connecting to the database.");
        }
        return conn;
    }

	/**
	 * Queries the tracks released by an artist before a year
	 * @param conn a connection object
	 * @param read scanner object to get user input
	 */
	public static void getTracksBeforeYear(Connection conn, Scanner read) throws SQLException {
		System.out.println("Enter an artist name: ");
		String artistName = read.nextLine();
		System.out.println("Enter a year(must be a number): ");
		int year = read.nextInt();
		read.nextLine();//clear input buffer
		String sqlStatement = "SELECT Name, Title " +
				"FROM FEATURES, TRACK, ARTIST " +
				"WHERE FEATURES.Artist_Id=ARTIST.Artist_Id AND ARTIST.Name=? AND FEATURES.Track_Title=TRACK.Title AND TRACK.Year<?;";
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.prepareStatement(sqlStatement);
			stmt.setString(1, artistName);
			stmt.setInt(2, year);
			rs = stmt.executeQuery();
			printResults(rs);
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		} finally {
			if(rs != null) {rs.close();}
			if(stmt != null) {stmt.close();}
		}
	}

	/**
	 * List all the albums and their unique identifiers with less than n copies held by the library.
	 * @param conn a connection object
	 * @param read scanner object to get user input
	 */
	public static void getAlbumCopies(Connection conn, Scanner read) throws SQLException {
		System.out.println("This query will list the albums that have fewer than n copies, where n is a parameter you provide.");
		System.out.print("Enter n: ");
		int n = read.nextInt();
		read.nextLine(); //clear input buffer
		String sqlStatement = "SELECT Media_ID, Name, COUNT(M.Name) as Count\n" +
				"FROM MEDIA AS M\n" +
				"WHERE Type_of_Media='Album'\n" +
				"GROUP BY M.Name\n" +
				"HAVING Count < ?;\n";
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.prepareStatement(sqlStatement);
			stmt.setInt(1, n);
			rs = stmt.executeQuery();
			printResults(rs);
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		} finally {
			if(rs != null) {rs.close();}
			if(stmt != null) {stmt.close();}
		}
	}

	/**
	 * Get albums checked out by patron
	 * @param conn a connection object
	 * @param read scanner object to get user input
	 */
	public static void getNumOfAlbumsCheckedOutByPatron(Connection conn, Scanner read) throws SQLException {
		System.out.print("Enter patron's card id: ");
		int n = read.nextInt();
		read.nextLine(); //clear input buffer
		String sqlStatement = "SELECT COUNT(M.Media_ID) as totalAlbums " +
				"FROM CHECK_OUT AS C, PATRON AS P, MEDIA AS M, LIBRARY_ITEM AS L " +
				"WHERE P.Card_ID = C.Card_ID AND M.Media_Id = L.Media_Id AND L.Item_id = C.Item_Id " +
				"AND M.Type_Of_Media = 'Album' AND P.Card_Id = ?;";
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.prepareStatement(sqlStatement);
			stmt.setInt(1, n);
			rs = stmt.executeQuery();
			printResults(rs);
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		} finally {
			if(rs != null) {rs.close();}
			if(stmt != null) {stmt.close();}
		}
	}

	/**
	 * Get games checked out by patron
	 * @param conn a connection object
	 * @param read scanner object to get user input
	 */
	public static void getNumOfGamesCheckedOutByPatron(Connection conn, Scanner read) throws SQLException {
		System.out.print("Enter patron's card id: ");
		int n = read.nextInt();
		read.nextLine(); //clear input buffer
		String sqlStatement = "SELECT COUNT(M.Media_ID) as totalGames " +
				"FROM CHECK_OUT AS C, PATRON AS P, MEDIA AS M, LIBRARY_ITEM AS L " +
				"WHERE P.Card_ID = C.Card_ID AND M.Media_Id = L.Media_Id AND L.Item_id = C.Item_Id " +
				"AND M.Type_Of_Media = 'Game' AND P.Card_Id = ?;";
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.prepareStatement(sqlStatement);
			stmt.setInt(1, n);
			rs = stmt.executeQuery();
			printResults(rs);
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		} finally {
			if(rs != null) {rs.close();}
			if(stmt != null) {stmt.close();}
		}
	}

	/**
	 * Displays the longest audiobook in the database along with its name and author.
	 * @param conn a connection object
	 */
	public static void getLongestAudiobook(Connection conn) throws SQLException {
		String sqlStatement = "SELECT M.Name, A.Name AS Author, Length AS Length_in_mins\n" +
				"FROM AUTHOR AS A, AUTHORS, MEDIA AS M\n" +
				"WHERE A.Author_Id = AUTHORS.Author_Id AND M.Type_of_Media = 'Audiobook' AND M.Media_Id = AUTHORS.Audiobook_Id\n" +
				"ORDER BY LENGTH DESC\n" +
				"LIMIT 1;\n";
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.prepareStatement(sqlStatement);
			rs = stmt.executeQuery();
			printResults(rs);
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		} finally {
			if(rs != null) {rs.close();}
			if(stmt != null) {stmt.close();}
		}
	}

	/**
	 * Retrieve the number of digital copies of every album in the library, along with the album and artist name.
	 * @param conn a connection object
	 */
	public static void getNumOfDigitalAlbumsCopies(Connection conn) throws SQLException {
		String sqlStatement = "SELECT M.Name, A.Name, COUNT(L.Is_Digital) AS Digital_Copies " +
				"FROM MEDIA AS M, LIBRARY_ITEM AS L, ARTIST AS A, ALBUM AS AL " +
				"WHERE M.Media_Id = L.Media_Id AND A.Artist_Id = AL.Artist_Id AND AL.Album_Id = " +
				"M.Media_Id AND Type_of_Media = 'Album' AND L.Is_Digital=1 " +
				"GROUP BY A.Name;";
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.prepareStatement(sqlStatement);
			rs = stmt.executeQuery();
			printResults(rs);
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		} finally {
			if(rs != null) {rs.close();}
			if(stmt != null) {stmt.close();}
		}
	}

	/**
	 * Insert a new audiobook into the database
	 * @param conn a connection object
	 * @param read scanner object to get user input
	 */
	public static void insertAudiobook(Connection conn, Scanner read) throws SQLException {
		//get max audiobook_id from AUDIOBOOK table and add 1 to it for the new audiobook(might not need to do this if we can figure out how to do auto-increment?);
		String query = "SELECT MAX(Audiobook_Id) " +
				"FROM AUDIOBOOK;";
		ResultSet rs = sqlQuery(conn, query);
		int id;
		if (rs.next()) {
			id = rs.getInt(0) + 1;
			if (id > MAX_AUDIOBOOK_ID) {
				System.out.println("Too many audiobooks!"); // TODO: More robust fix
				return;
			}
		} else {
			id = MIN_AUDIOBOOK_ID;
		}
		rs.close();

		//list current narrators
		query = "SELECT Name " +
				"FROM NARRATOR" +
				"ORDER BY Narrator_Id;";
		rs = sqlQuery(conn, query);
		printResults(rs);
		rs.close();

		//ask which narrator to select
		System.out.println("Enter the id of the narrator you want (Enter -1 for a new narrator [WIP])."); // TODO: These two WIPs
		int narrId = read.nextInt();
		read.nextLine();

		//list current authors
		query = "SELECT Name " +
				"FROM AUTHOR" +
				"ORDER BY Author_Id;";
		rs = sqlQuery(conn, query);
		printResults(rs);
		rs.close();

		//ask them to select which authors contributed to the audiobook
		System.out.println("Enter the id of the author you want (Enter -1 for a new author [WIP])."); // TODO: ^
		int authId = read.nextInt();
		read.nextLine();

		//get values for MEDIA entry
		System.out.println("Enter the following information:");
		System.out.print("Name: ");
		String name = read.nextLine();
		System.out.print("Genre: ");
		String genre = read.nextLine();
		System.out.print("Year: ");
		int year = read.nextInt();
		read.nextLine();
		System.out.print("Length: ");
		int length = read.nextInt();
		read.nextLine();

		//get values for AUDIOBOOK entry
		System.out.print("Chapters: ");
		int chapters = read.nextInt();
		read.nextLine();

		//create media entry, then audiobook entry(using narrator they provide)
		query = "INSERT INTO MEDIA " +
				"VALUES (?, ?, ?, ?, ?, ?);";
		PreparedStatement stmt = null;
		rs = null;
		try {
			stmt = conn.prepareStatement(query);
			stmt.setInt(1, id);
			stmt.setString(2, name);
			stmt.setString(3, genre);
			stmt.setInt(4, year);
			stmt.setInt(5, length);
			stmt.setString(6, "Audiobook");
			rs = stmt.executeQuery();
			printResults(rs);
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		} finally {
			if(rs != null) {rs.close();}
			if(stmt != null) {stmt.close();}
		}

		query = "INSERT INTO AUDIOBOOK " +
				"VALUES (?, ?, ?);";
		stmt = null;
		rs = null;
		try {
			stmt = conn.prepareStatement(query);
			stmt.setInt(1, id);
			stmt.setInt(2, chapters);
			stmt.setInt(3, narrId);
			rs = stmt.executeQuery();
			printResults(rs);
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		} finally {
			if(rs != null) {rs.close();}
			if(stmt != null) {stmt.close();}
		}

		//add relevant authors to AUTHORS table along with audiobook ID.
		query = "INSERT INTO AUTHORS " +
				"VALUES (?, ?);";
		stmt = null;
		rs = null;
		try {
			stmt = conn.prepareStatement(query);
			stmt.setInt(1, id);
			stmt.setInt(2, authId);
			rs = stmt.executeQuery();
			printResults(rs);
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		} finally {
			if(rs != null) {rs.close();}
			if(stmt != null) {stmt.close();}
		}
	}

	/**
	 * Takes result set object and prints it
	 * @param rs result set to print
	 */
	public static void printResults(ResultSet rs) throws SQLException {
		try{
			ResultSetMetaData rsmd = rs.getMetaData();
			int columnCount = rsmd.getColumnCount();
			for (int i = 1; i <= columnCount; i++) {
				String value = rsmd.getColumnName(i);
				System.out.print(value);
				if (i < columnCount) System.out.print(",  ");
			}
			System.out.print("\n");
			while (rs.next()) {
				for (int i = 1; i <= columnCount; i++) {
					String columnValue = rs.getString(i);
					System.out.print(columnValue);
					if (i < columnCount) System.out.print(",  ");
				}
				System.out.print("\n");
			}
		}catch (SQLException e) {
			System.out.println(e.getMessage());
		}
	}

	/**
	 * Prints out the list of actors
	 *
	 * @param conn a connection object
	 */
	public static void getActors(Connection conn) throws SQLException {
		String sql = "SELECT * FROM ACTOR;";
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.prepareStatement(sql);
			rs = stmt.executeQuery();
			printResults(rs);
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		} finally {
			if(rs != null) {rs.close();}
			if(stmt != null) {stmt.close();}
		}
	}

	/**
     * Queries the database and prints the results.
     * 
     * @param conn a connection object
     * @param sql a SQL statement that returns rows
     */
    public static ResultSet sqlQuery(Connection conn, String sql) throws SQLException {
    	PreparedStatement stmt = null;
    	ResultSet rs = null;
        try {
        	stmt = conn.prepareStatement(sql);
        	rs = stmt.executeQuery();
        } catch (SQLException e) {
			System.out.println(e.getMessage());
		}
        return rs;
    }

    public static void main(String[] args) {
    	Connection conn = initializeDB(DATABASE);
    	Scanner userInput = new Scanner(System.in);
    	System.out.println("\nWelcome to our application. Enter the corresponding integer to the query you want to run. \n");
    	System.out.println("1. Find the titles of all tracks by ARTIST released before YEAR");
		System.out.println("2. List all the albums and their unique identifiers with less than N copies held by the library.");
		System.out.println("3. Select all actors.");
		System.out.println("4. Get number of albums checked out by a patron.");
		System.out.println("5. Insert a new audiobook.");
		System.out.println("6. Get number of games checked out by a patron.");
		System.out.println("7. Retrieve the number of digital copies of every album in the library, along with the album and artist name.");
		System.out.println("8. Display the longest audiobook in the database along with its name and author.");
		System.out.println("Enter the corresponding number: ");
		int choice = userInput.nextInt();
		userInput.nextLine();
		try {
			switch(choice) {
				case 1:
					getTracksBeforeYear(conn, userInput);
					break;
				case 2:
					getAlbumCopies(conn, userInput);
					break;
				case 3:
					getActors(conn);
					break;
				case 4:
					getNumOfAlbumsCheckedOutByPatron(conn, userInput);
					break;
				case 5:
					insertAudiobook(conn, userInput);
					break;
				case 6:
					getNumOfGamesCheckedOutByPatron(conn, userInput);
					break;
				case 7:
					getNumOfDigitalAlbumsCopies(conn);
					break;
				case 8:
					getLongestAudiobook(conn);
					break;
				default:
					System.out.println("Incorrect input");
			}
		} catch (SQLException e) {
			e.getMessage();
		}

	}
}
