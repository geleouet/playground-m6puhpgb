package com.yourself;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.Test;
public class H2Test {

	static class ColumnRes {
		List<String> values = new ArrayList<>();
		final String name;
		final Function<Object, String> mapper;
		int max = 0;
		
		public ColumnRes(String columnName, Function<Object, String> mapper) {
			this.name=columnName;
			this.mapper = mapper;
			max = this.name.length() + 1;
		}
		
		void add(Object o)   {
			String apply = mapper.apply(o);
			max = Math.max(max, apply.length());
			values.add(apply);
		}
	}
	
	@Test
	public void test() throws Exception {
		Connection conn = DriverManager.getConnection("jdbc:h2:mem:", "sa", "");

		createDeals(conn);
		createPrices(conn);
		
		
//		ResultSet executeQuery = conn.prepareStatement("SELECT * FROM DEAL").executeQuery();
		String result="";
		FileInputStream fis = new FileInputStream("universe.sql");
		try(BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fis))) {
			result = bufferedReader.lines().collect(Collectors.joining("\n"));
		}
		ResultSet executeQuery = conn.prepareStatement(result).executeQuery();
		int columnCount = executeQuery.getMetaData().getColumnCount();
		
		ColumnRes[] columns = new ColumnRes[columnCount];
		
		for (int i = 1; i <= columnCount; i++) {
			String columnClassName = executeQuery.getMetaData().getColumnClassName(i);
			
			String columnName = executeQuery.getMetaData().getColumnName(i);
			columns[i-1] = createResColumn(columnClassName, columnName);
		}
		
		
		int nbRow = 0;
		while (executeQuery.next()) {
			for (int i = 1; i <= columnCount; i++) {
				columns[i-1].add(executeQuery.getObject(i));
			}
			nbRow ++;
		}

		System.out.println(format(columnCount, nbRow, columns));
		
		conn.close();
	}

	private StringBuilder format(int columnCount, int rowCount, ColumnRes[] columns) {
		StringBuilder header = new StringBuilder();
		StringBuilder line = new StringBuilder();
		for (int i = 0; i< columnCount; i++) {
			if (i!=0) {
				header.append('|');
				line.append('|');
			}
			String n = columns[i].name + BLANK.substring(0, columns[i].max - columns[i].name.length());
			header.append(n);
			line.append(LINE.substring(0, columns[i].max));
			
		}
		StringBuilder out = new StringBuilder();
		out.append(header).append("\n");
		out.append(line).append("\n");

		for (int j = 0; j < rowCount; j++) {
			for (int i = 0; i< columnCount; i++) {
				if (i!=0) {
					out.append('|');
				}
				String n = columns[i].values.get(j) + BLANK.substring(0, columns[i].max - columns[i].values.get(j).length());
				out.append(n);
			}
			out.append("\n");
		}
		return out;
	}

	
	private final static String BLANK = "                                ";
	private final static String LINE  = "--------------------------------";
	
	private ColumnRes createResColumn(String columnClassName, String columnName) {
		Function<Object, String> mapper;
		switch (columnClassName) {
		case "java.lang.Integer":
			mapper = o -> NumberFormat.getInstance().format((Integer) o);
			break;
		case "java.lang.Double":
			mapper = o -> new DecimalFormat().format((Double) o);
			break;
		case "java.lang.String":
			mapper = o -> ((String) o).length() > 16 ? (((String) o).substring(0, 13)+"...") : o.toString();
			break;

		default:
			mapper = o -> o!= null ? o.toString() : "NULL";
			break;
		}
		return new ColumnRes(columnName, mapper);
	}

	private void createPrices(Connection conn) throws SQLException {
		conn.prepareStatement(
				"CREATE TABLE PRICE ("
						+ "  DEALID INT PRIMARY KEY,"
						+ "  PRICE FLOAT"
						+ ");"
				).execute();
		conn.prepareStatement("INSERT INTO PRICE VALUES(1, 0.3)").execute();
		conn.prepareStatement("INSERT INTO PRICE VALUES(2, 4)").execute();
		conn.prepareStatement("INSERT INTO PRICE VALUES(3, 7)").execute();
		conn.prepareStatement("INSERT INTO PRICE VALUES(4, 10)").execute();
		conn.prepareStatement("INSERT INTO PRICE VALUES(5, 1.6)").execute();
		conn.prepareStatement("INSERT INTO PRICE VALUES(6, 8)").execute();
	}

	private void createDeals(Connection conn) throws SQLException {
		conn.prepareStatement(
				"CREATE TABLE DEAL ("
				+ "  DEALID INT PRIMARY KEY,"
				+ "  NAME VARCHAR"
				+ ");"
						).execute();
		conn.prepareStatement("INSERT INTO DEAL VALUES(1, 'A')").execute();
		conn.prepareStatement("INSERT INTO DEAL VALUES(2, 'B')").execute();
		conn.prepareStatement("INSERT INTO DEAL VALUES(3, 'C')").execute();
		conn.prepareStatement("INSERT INTO DEAL VALUES(4, 'D')").execute();
		conn.prepareStatement("INSERT INTO DEAL VALUES(5, 'E')").execute();
		conn.prepareStatement("INSERT INTO DEAL VALUES(6, 'F')").execute();
	}
	
}
