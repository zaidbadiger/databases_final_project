import java.sql.*;
import java.util.ArrayList;
import java.util.List;
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
	private static String DATABASE = "Library_Database.db";

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
		String sqlStatement = "SELECT M.Media_ID, Name, COUNT(L.Media_Id) as Count\n" +
				"FROM MEDIA AS M, LIBRARY_ITEM AS L\n" +
				"WHERE L.Media_Id=M.Media_Id AND M.Type_of_Media='Album'\n" +
				"GROUP BY L.Media_Id\n" +
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
		String sqlStatement = "SELECT P.First_Name, P.Card_Id, COUNT(L.Media_ID) as totalAlbumsCheckedOut\n" +
				"FROM CHECK_OUT AS C, PATRON AS P, MEDIA AS M, LIBRARY_ITEM AS L\n" +
				"WHERE P.Card_ID = C.Card_ID AND C.Item_Id=L.Item_Id AND L.Media_ID = M.Media_ID AND M.Type_Of_Media = 'Album' AND C.Card_Id = ?;\n";
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
		String sqlStatement = "SELECT P.First_Name, P.Card_Id, COUNT(M.Media_ID) as totalGames\n" +
				"FROM CHECK_OUT AS C, PATRON AS P, MEDIA AS M, LIBRARY_ITEM AS L\n" +
				"WHERE P.Card_ID = C.Card_ID AND M.Media_Id = L.Media_Id AND L.Item_id = C.Item_Id AND M.Type_Of_Media = 'Game' AND P.Card_Id = ?;\n";
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
		String sqlStatement = "SELECT M.Name AS Media_Name, A.Name AS Artist_Name, COUNT(L.Is_Digital) AS Digital_Copies\n" +
				"FROM MEDIA AS M, LIBRARY_ITEM AS L, ARTIST AS A, ALBUM AS AL\n" +
				"WHERE M.Media_Id = L.Media_Id AND A.Artist_Id = AL.Artist_Id AND AL.Album_Id = M.Media_Id AND Type_of_Media = 'Album' AND L.Is_Digital=1\n" +
				"GROUP BY A.Name;\n";
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
	 * Get all of the Ids and names in a table by passing a ResultSet(Narrator, Author, Director, etc...)
	 * @param rs a ResultSet object on a query that returns all names from a table
	 */
	public static List<Contributor> getIdAndNames(ResultSet rs) throws SQLException {
		List<Contributor> names = new ArrayList<Contributor>();
		try{
			while (rs.next()) {
				Contributor contributor = new Contributor();
				contributor.Id=rs.getInt(1);
				contributor.name=rs.getString(2);
				names.add(contributor);
			}
		}catch (SQLException e) {
			System.out.println(e.getMessage());
		}
		return names;
	}

	/**
	 * NOTE: NOT AN AUTHOR, PERTAINS TO "AUTHORS" TABLE
	 * Insert a new authors row into the database
	 * @param conn a connection object
	 * @param audiobookId
	 * @param authorId
	 * @param
	 */
	public static void insertAuthors(Connection conn, int audiobookId, int authorId) throws SQLException {
		String query = "INSERT INTO AUTHORS " +
				" VALUES (?, ?);";
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement(query);
			stmt.setInt(1, audiobookId);
			stmt.setInt(2, authorId);
			stmt.executeUpdate();
			System.out.println("Successfully added to AUTHORS table");

		} catch (SQLException e) {
			System.out.println(e.getMessage());
		} finally {
			if(stmt != null) {stmt.close();}
		}
	}
	/**
	 * Insert a new narrator into the database
	 * @param conn a connection object
	 * @param tableName name of table
	 * @param name name of contributor
	 * @param
	 */
	public static void insertContributor(Connection conn, String tableName, String name, int Id) throws SQLException {
		String query = "INSERT INTO " + tableName +
				" VALUES (?, ?);";
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement(query);
			stmt.setString(1, name);
			stmt.setInt(2, Id);
			stmt.executeUpdate();
			System.out.println("Successfully added contributor");

		} catch (SQLException e) {
			System.out.println(e.getMessage());
		} finally {
			if(stmt != null) {stmt.close();}
		}
	}

	/**
	 * Insert a new audiobook into the database
	 * @param conn a connection object
	 * @param read scanner object to get user input
	 */
	public static void insertAudiobook(Connection conn, Scanner read) throws SQLException {
		conn.setAutoCommit(false);
		String query = "SELECT MAX(Audiobook_Id) " +
				"FROM AUDIOBOOK;";
		ResultSet rs = sqlQuery(conn, query);
		int id;
		if (rs.next()) {
			id = rs.getInt(1) + 1;
			if (id > MAX_AUDIOBOOK_ID) {
				System.out.println("Too many audiobooks!");
				return;
			}
		} else {
			id = MIN_AUDIOBOOK_ID;
		}
		rs.close();

		//list current narrators
		//save current narrators in a list
		query = "SELECT Narrator_Id, Name " +
				"FROM NARRATOR;";
		rs = sqlQuery(conn, query);
		List<Contributor> narrators = getIdAndNames(rs);
		rs.close();

		//ask which narrator to select
		System.out.print("Enter the name of the narrator for the audiobook : ");
		String narratorName = read.nextLine();
		int narratorId=-1;
		//check if narrator already exists, if not, insert narrator
		//save narratorId
		for (Contributor narrator : narrators) {
			if (narrator.name.equals(narratorName)) {
				System.out.println("Wonderful, the narrator already exists in the database");
				narratorId = narrator.Id;
				break;
			}
		}
		if(narratorId==-1){//add narrator and save Id
			//get narrator Id
			query = "SELECT MAX(Narrator_Id) " +
					"FROM NARRATOR;";
			ResultSet maxSet = sqlQuery(conn, query);
			if (maxSet.next()) {
				narratorId = maxSet.getInt(1) + 1;
			} else {
				narratorId = 0;
			}
			maxSet.close();
			//add narrator
			insertContributor(conn, "NARRATOR", narratorName, narratorId);
		}


		//get values for MEDIA entry
		System.out.println("Enter the following information for the audiobook:");
		System.out.print("Name: ");
		String name = read.nextLine();
		System.out.print("Genre: ");
		String genre = read.nextLine();
		System.out.print("Year: ");
		int year = read.nextInt();
		read.nextLine();
		System.out.print("Length in minutes as an integer: ");
		int length = read.nextInt();
		read.nextLine();

		//create media entry, then audiobook entry(using narrator they provide)
		query = "INSERT INTO MEDIA " +
				"VALUES (?, ?, ?, ?, ?, ?);";
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement(query);
			stmt.setInt(1, id);
			stmt.setString(2, name);
			stmt.setString(3, genre);
			stmt.setInt(4, year);
			stmt.setInt(5, length);
			stmt.setString(6, "Audiobook");
			stmt.executeUpdate();

		} catch (SQLException e) {
			System.out.println(e.getMessage());
		} finally {
			if(stmt != null) {stmt.close();}
		}

		query = "INSERT INTO AUDIOBOOK " +
				"VALUES (?, ?);";
		stmt = null;
				try {
			stmt = conn.prepareStatement(query);
			stmt.setInt(1, id);
			stmt.setInt(2, narratorId);
			stmt.executeUpdate();

		} catch (SQLException e) {
			System.out.println(e.getMessage());
		} finally {
			if(stmt != null) {stmt.close();}
		}


		//ask them to select which authors contributed to the audiobook
		System.out.print("How many authors does this book have? Enter an integer: ");
		int numAuthors = read.nextInt();
		read.nextLine();
		String authorName;
		//save current authors in a list
		query = "SELECT Author_Id, Name " +
				"FROM AUTHOR;";
		rs = sqlQuery(conn, query);
		List<Contributor> authors = getIdAndNames(rs);
		rs.close();
		int authorId;
		for(int i=0; i<numAuthors; i++) {
			System.out.print("Enter the name of an author that contributed to the book, make sure not to repeat authors: ");
			authorName = read.nextLine();
			authorId=-1;
			//check if author already exists, if not, insert author
			//save authorId
			for (Contributor author : authors) {
				if (author.name.equals(authorName)) {
					System.out.println("Wonderful, the author already exists in the database");
					authorId = author.Id;
					break;
				}
			}
			if(authorId==-1){//add author and save Id
				//get author Id
				query = "SELECT MAX(Author_Id) " +
						"FROM AUTHOR;";
				ResultSet maxSet = sqlQuery(conn, query);
				if (maxSet.next()) {
					authorId = maxSet.getInt(1) + 1;
				} else {
					authorId = 0; //no authors in db
				}
				maxSet.close();
				//add author
				insertContributor(conn, "AUTHOR", authorName, authorId);
			}
			//create authors entry
			insertAuthors(conn, id, authorId);
		}
		conn.commit();
		conn.setAutoCommit(true);
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
