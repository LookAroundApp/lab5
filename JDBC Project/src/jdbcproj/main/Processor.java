package jdbcproj.main;

import java.sql.*;

public class Processor {

	private Connection c;
	protected String url, name, password;

	/*
	 * A simple constructor - try to connect the server with given params
	 */
	Processor(String url, String name, String password)
			throws ClassNotFoundException {
		Class.forName("com.mysql.jdbc.Driver");
		try {
			this.connect(url, name, password);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/*
	 * The following method creates stored procedures if they not exist in data
	 * base, which is just opened
	 */
	public void createProcedures() {
		Statement st;
		try {
			// create stored procedure newtable
			st = c.createStatement();
			String sql = "DROP PROCEDURE IF EXISTS newtable;\n";
			st.execute(sql);
			sql = "CREATE PROCEDURE newtable (IN name VARCHAR(255), IN fields TEXT)\n"
					+ "BEGIN\n"
					+ "SET @query = CONCAT('CREATE TABLE IF NOT EXISTS ', name,' (', fields, ')');\n"
					+ "PREPARE stmt from @query;\n"
					+ "EXECUTE stmt;\n"
					+ "DEALLOCATE PREPARE stmt;\n" + "END;";
			st.execute(sql);

			// create stored procedure droptable
			st = c.createStatement();
			sql = "DROP PROCEDURE IF EXISTS droptable;\n";
			st.execute(sql);
			sql = "CREATE PROCEDURE droptable (IN name VARCHAR(255))\n"
					+ "BEGIN\n"
					+ "SET @query = CONCAT('DROP TABLE IF EXISTS ', name);\n"
					+ "PREPARE stmt from @query;\n" + "EXECUTE stmt;\n"
					+ "DEALLOCATE PREPARE stmt;\n" + "END;";
			st.execute(sql);

			// create stored procedure addrow
			st = c.createStatement();
			sql = "DROP PROCEDURE IF EXISTS addrow;\n";
			st.execute(sql);
			sql = "CREATE PROCEDURE addrow (IN name VARCHAR(255), IN values TEXT)\n"
					+ "BEGIN\n"
					+ "SET @query = CONCAT('INSERT INTO ', name, ' VALUES (', values, ')');\n"
					+ "PREPARE stmt from @query;\n"
					+ "EXECUTE stmt;\n"
					+ "DEALLOCATE PREPARE stmt;\n" + "END;";
			st.execute(sql);

			// create stored procedure updatetable
			st = c.createStatement();
			sql = "DROP PROCEDURE IF EXISTS updatetable;\n";
			st.execute(sql);
			sql = "CREATE PROCEDURE updatetable (IN name VARCHAR(255), IN params TEXT, IN search TEXT)\n"
					+ "BEGIN\n"
					+ "SET @query = CONCAT('UPDATE ', name, ' SET ', params, ' WHERE ', search);\n"
					+ "PREPARE stmt from @query;\n"
					+ "EXECUTE stmt;\n"
					+ "DEALLOCATE PREPARE stmt;\n" + "END;";
			st.execute(sql);

			// create stored procedure remove
			st = c.createStatement();
			sql = "DROP PROCEDURE IF EXISTS remove;\n";
			st.execute(sql);
			sql = "CREATE PROCEDURE remove (IN name VARCHAR(255), IN search TEXT)\n"
					+ "BEGIN\n"
					+ "SET @query = CONCAT('DELETE FROM ', name, ' WHERE ', search);\n"
					+ "PREPARE stmt from @query;\n"
					+ "EXECUTE stmt;\n"
					+ "DEALLOCATE PREPARE stmt;\n" + "END;";
			st.execute(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/*
	 * Check if connected to DB
	 */
	public boolean isConnected() {
		return this.c == null ? false : true;
	}

	/*
	 * This method creates DB with <param>name</param>. DB creation can't be
	 * done using stored procedures because they have to be stored in th DB, so
	 * there is just an SQL query
	 */
	public boolean createDB(String name) {
		boolean res = false;
		try {
			Statement st = c.createStatement();
			String sql = "CREATE DATABASE " + name + ";";
			st.execute(sql);
			res = true;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return res;
	}

	/*
	 * Table creation: call stored proc with IN params created before, return
	 * false in case of error
	 */
	public boolean createTable(String name, String[] fields) {
		boolean res = false;
		try {
			String f = String.join(" VARCHAR(255), ", fields) + " VARCHAR(255)";
			PreparedStatement st = c.prepareStatement("{call newtable(?,?)}");
			st.setString(1, name);
			st.setString(2, f);
			st.executeQuery();
			res = true;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return res;
	}

	private boolean dropTable() {

		return true;
	}

	private boolean addRow() {

		return true;
	}

	private boolean search() {

		return true;
	}

	private boolean removeRows() {

		return true;
	}

	private boolean updateRows() {

		return true;
	}

	/*
	 * Establish new connection to DB with params, sets global vars
	 */
	public void connect(String url, String name, String password)
			throws SQLException {
		Connection con = DriverManager.getConnection(url, name, password);
		System.out.println("Connected to " + url);
		this.c = con;
		this.url = url;
		this.name = name;
		this.password = password;
	}

	/*
	 * Break the connection
	 */
	public void disconnect() throws SQLException {
		if (this.isConnected()) {
			this.c.close();
		}
		System.out.println("Disconnected.");
	}
}
