package jdbcproj.main;

import java.sql.*;
import java.util.Scanner;

public class Main {

	public static void main(String[] args) throws SQLException,
			ClassNotFoundException {

		String url = "";
		String name = "";
		String password = "";
		Processor cp;
		Scanner inp = new Scanner(System.in);

		if ((args.length == 0)
				|| (args.length == 1 && args[0].equals("default"))) {
			url = "jdbc:mysql://localhost:3306/";
			name = "admin";
			password = "root";
		} else if (args.length == 1
				&& (args[0].equals("--help") || args[0].equals("-h")
						|| args[0].equals("--usage") || args[0]
							.equals("-usage"))) {
			Main.usage();
		} else if (args.length == 3) {
			url = args[0];
			name = args[1];
			password = args[2];
		} else {
			Main.usage();
		}

		cp = new Processor(url, name, password);
		
		while (true) {
			String req = getRequest(inp);
			if (req.equals("exit")) {
				cp.disconnect();
				break;
			}
			proceedRequest(req, cp);
		}

	}

	private static String getRequest(Scanner s) {
		return s.nextLine();
	}

	private static void proceedRequest(String request, Processor cp)
			throws SQLException {
		String[] parts = request.split(" ");
		String command = parts[0];
		switch (command) {
		case "create":
			if (parts.length == 2) {
				if (cp.createDB(parts[1])) {
					System.out.println("Database " + parts[1] + " created");
				} else {
					System.out.println("Something wrong: database " + parts[1]
							+ " was not created");
				}
			} else {
				wrongCommand(request);
			}
			break;
		case "connect":
			if (parts.length == 2) {
				if (cp.isConnected()) {
					cp.disconnect();
					cp.connect(cp.url + parts[1], cp.name, cp.password);
					cp.createProcedures();
				}
			} else {
				wrongCommand(request);
			}
			break;
		case "table":
			if (parts.length >= 3) {
				String name = parts[1];
				String[] fields = new String[parts.length - 2];
				for (int i = 2; i < parts.length; i++) {
					fields[i - 2] = parts[i];
				}
				if (cp.createTable(name, fields)) {
					System.out.println("Success");
				}
			} else {
				wrongCommand(request);
			}
			break;
		}
	}

	private static void wrongCommand(String req) {
		System.out.println("Wrong command or number of args: " + req);
	}

	private static void usage() {
		ln("MySQL Java light interface.");
		ln("Based on JDBC.");
		ln("Usage:");
		ln("Start:");
		ln("\tlightsql default              - use default server settings");
		ln("\tlightsql server user password - use specified params.");
		ln("Database operations:");
		ln("\tcreate name                     - create new data base \"name\"");
		ln("\tfire name                       - remove \"name\" data base");
		ln("\tconnect db                      - establish new connection to \"db\"");
		ln("\ttable t f1..fN                  - create table \"t\" with fields \"f[0]\" - \"f[N]\"");
		ln("\tdrop table                      - clear \"table\"");
		ln("\tadd table v1..vN                - add new row with values \"v[0]\" - \"v[N]\" into \"table\"");
		ln("\tsearch table field pattern      - perform a search in \"table\" by \"field\"");
		ln("\t                                  the result is stored in memory");
		ln("\tmemfree                         - clean up memory after search");
		ln("\tupdate f1:v1 fN:vN              - update memory, set value \"v[i]\" to field\"f[i]\" ");
		ln("\tremove                          - remove rows (latest search result)");
		ln("End session: exit");
		ln("\nauthor: vladimir.gerasimov@intel.com");
		System.exit(0);
	}

	private static void ln(String s) {
		System.out.println(s);
	}

}
