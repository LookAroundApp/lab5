package jdbcproj.main;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class Memory {
	String col;
	String needle;
	String table;
	List<List<String>> rs = new ArrayList();

	Memory(String table, String col, String needle, ResultSet res)
			throws SQLException {
		this.table = table;
		this.col = col;
		this.needle = needle;
		ResultSetMetaData md = res.getMetaData();
		int col_n = md.getColumnCount();
		while (res.next()) {
			List<String> row = new ArrayList();
			for (int i = 1; i <= col_n; i++) {
				row.add(res.getString(i));
			}
			this.rs.add(row);
		}
	}

	/*
	 * cycle in cycle in cycle
	 */
	public void print() {
		for (int i = 0; i < rs.size(); i++) {
			List<String> sub = rs.get(i);
			for (int t = 0; t < sub.size(); t++) {
				// lets set the maximum length of a cell to 20 symbols
				String v = sub.get(t);
				if (v.length() > 20) {
					v.substring(0, 20);
				}
				for (int k = 20 - v.length(); k >= 0; k--) {
					v += " ";
				}
				System.out.print(v);
			}
			System.out.print("\n");
		}
	}
}
