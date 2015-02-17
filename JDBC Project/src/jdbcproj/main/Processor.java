package jdbcproj.main;

import java.sql.*;

public class Processor {

	private Connection c;
	protected String url, name, password;
	protected Memory m;

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
			sql = "CREATE PROCEDURE addrow (IN name VARCHAR(255), IN val TEXT)\n"
					+ "BEGIN\n"
					+ "SET @query = CONCAT('INSERT INTO ', name, ' VALUES (', val, ')');\n"
					+ "PREPARE stmt from @query;\n"
					+ "EXECUTE stmt;\n"
					+ "DEALLOCATE PREPARE stmt;\n" + "END;";
			st.execute(sql);

			// create stored procedure updatetable
			st = c.createStatement();
			sql = "DROP PROCEDURE IF EXISTS updatetable;\n";
			st.execute(sql);
			sql = "CREATE PROCEDURE updatetable (IN name VARCHAR(255), IN params TEXT, IN col VARCHAR(255), IN search TEXT)\n"
					+ "BEGIN\n"
					+ "SET @query = CONCAT('UPDATE ', name, ' SET ', params, ' WHERE ', col, ' LIKE \"%', search, '%\"');\n"
					+ "PREPARE stmt from @query;\n"
					+ "EXECUTE stmt;\n"
					+ "DEALLOCATE PREPARE stmt;\n" + "END;";
			st.execute(sql);

			// create stored procedure remove
			st = c.createStatement();
			sql = "DROP PROCEDURE IF EXISTS removerows;\n";
			st.execute(sql);
			sql = "CREATE PROCEDURE removerows (IN name VARCHAR(255), IN col VARCHAR(255), IN search TEXT)\n"
					+ "BEGIN\n"
					+ "SET @query = CONCAT('DELETE FROM ', name, ' WHERE ', col, ' LIKE \"%', search, '%\"');\n"
					+ "PREPARE stmt from @query;\n"
					+ "EXECUTE stmt;\n"
					+ "DEALLOCATE PREPARE stmt;\n" + "END;";
			st.execute(sql);

			// create stored procedure search
			st = c.createStatement();
			sql = "DROP PROCEDURE IF EXISTS search;\n";
			st.execute(sql);
			sql = "CREATE PROCEDURE search (IN name VARCHAR(255), IN col VARCHAR(255), IN search VARCHAR(255))\n"
					+ "BEGIN\n"
					+ "SET @query = CONCAT('SELECT * FROM ', name, ' WHERE ', col, ' LIKE \"%', search, '%\"');\n"
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

	public boolean removeDB(String name) {
		boolean res = false;
		try {
			Statement st = c.createStatement();
			String sql = "DROP DATABASE " + name + ";";
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

	/*
	 * Drop table: call stored proc with IN param name
	 */
	public boolean dropTable(String name) {
		boolean res = false;
		try {
			PreparedStatement st = c.prepareStatement("{call droptable(?)}");
			st.setString(1, name);
			st.executeQuery();
			res = true;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return res;
	}

	/*
	 * Insert a new row into table
	 */
	public boolean addRow(String name, String[] values) {
		boolean res = false;
		try {
			String f = "";
			for (int i = 0; i < values.length; i++) {
				f += "'" + values[i] + "'";
				if (i < values.length - 1) {
					f += ", ";
				}
			}
			PreparedStatement st = c.prepareStatement("{call addrow(?,?)}");
			st.setString(1, name);
			st.setString(2, f);
			st.executeQuery();
			res = true;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return res;
	}

	/*
	 * Perform a search in <param>table</param> by column <param>col</param> by
	 * pattern <param>needle</param>
	 */
	public boolean search(String table, String col, String needle) {
		boolean res = false;
		try {
			CallableStatement st = c.prepareCall("{call search(?,?,?)}");
			st.setString(1, table);
			st.setString(2, col);
			st.setString(3, needle);
			ResultSet result = st.executeQuery();
			this.m = new Memory(table, col, needle, result);
			res = true;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return res;
	}

	/*
	 * This method takes the Memory object, gets data from it and deletes rows
	 */
	public boolean removeRows() {
		boolean res = false;
		if (this.m != null) {
			try {
				PreparedStatement st = c
						.prepareStatement("{call removerows(?,?,?)}");
				st.setString(1, this.m.table);
				st.setString(2, this.m.col);
				st.setString(3, this.m.needle);
				st.executeQuery();
				this.clearMemory();
				res = true;
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return res;
	}

	public boolean updateRows(String[] f, String[] v) {
		boolean res = false;
		String param = "";
		for (int i = 0; i < f.length; i++) {
			param += f[i] + "='" + v[i] + "'";
			if (i < f.length - 1) {
				param += ", ";
			}
		}

		if (this.m != null) {
			try {
				PreparedStatement st = c
						.prepareStatement("{call updatetable(?,?,?,?)}");
				st.setString(1, this.m.table);
				st.setString(2, param);
				st.setString(3, this.m.col);
				st.setString(4, this.m.needle);
				st.executeQuery();
				this.clearMemory();
				res = true;
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return res;
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

	public void showMemory() {
		if (this.m != null) {
			this.m.print();
		}
	}

	public void clearMemory() {
		this.m = null;
	}
}
