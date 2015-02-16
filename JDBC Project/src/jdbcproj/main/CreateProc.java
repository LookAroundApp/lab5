package jdbcproj.main;

import java.sql.*;

public class CreateProc {

	private Connection c;
	protected String url, name, password;

	CreateProc(String url, String name, String password)
			throws ClassNotFoundException {
		Class.forName("com.mysql.jdbc.Driver");
		try {
			this.connect(url, name, password);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void createProcedures() {
		Statement st;
		try {
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
		} catch (SQLException e) {
			e.printStackTrace();

		}
	}

	public boolean isConnected() {
		return this.c == null ? false : true;
	}

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

	private boolean connectDB() {

		return true;
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

	public void connect(String url, String name, String password)
			throws SQLException {
		Connection con = DriverManager.getConnection(url, name, password);
		System.out.println("Connected to " + url);
		this.c = con;
		this.url = url;
		this.name = name;
		this.password = password;
	}

	public void disconnect() throws SQLException {
		this.c.close();
		System.out.println("Disconnected.");
	}
}
