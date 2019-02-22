package com.yourself;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.Assert;
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
			String apply = format(o);
			max = Math.max(max, apply.length());
			values.add(apply);
		}

		private String format(Object o) {
			try {
				return mapper.apply(o);
			} catch (Exception e) {
				e.printStackTrace();
			}
			return null;
		}
	}
	
	static class ResultQuery {
		int columnCount;
		ColumnRes[] columns;
		int nbRow = 0;
		
	}
	
	@Test
	public void test() throws Exception {
		String fileName = "universe.sql";

//		-- SQL
//		SELECT NAME, isnull(sum(Price), 0) as TOTAL FROM DEAL
//		left outer join price on deal.dealId = price.dealId
//		group by Name;



		Connection conn = DriverManager.getConnection("jdbc:h2:mem:", "sa", "");
		//createDeals(conn);
		createPrices(conn);
		
		String request = readInput(fileName);

		ResultQuery rq = executeQuery(conn, request);
		msg("SQL", format(rq));
		conn.close();

		try {
			Assert.assertEquals("First column should be named 'name'", "name", rq.columns[0].name.toLowerCase());
			Assert.assertEquals("Second column should be named 'total'", "total", rq.columns[1].name.toLowerCase());
			Assert.assertEquals("Names should be sorted", "A", rq.columns[0].values.get(0));
			Assert.assertEquals("Names should be sorted", "B", rq.columns[0].values.get(1));
			Assert.assertEquals("Value for B should be '14'", "14", rq.columns[1].values.get(1));
			Assert.assertEquals("Value for D should be displayed", "D", rq.columns[0].values.get(3));
			Assert.assertEquals("Value for D should be '0'", "0", rq.columns[1].values.get(3));
		} catch (AssertionError ae) {
			success(false);
			msg("Oops! 🐞", ae.getMessage());
		}
		
	}
	
	
	private static void msg(String channel, String msgs) {
		Arrays.stream(msgs.split("\n")).forEach(msg -> 
			System.out.println(String.format("TECHIO> message --channel \"%s\" \"%s\"", channel, msg))
		);
	}

	private static void success(boolean success) {
		System.out.println(String.format("TECHIO> success %s", success));
	}

	private String format(ResultQuery rq) {
		int columnCount = rq.columnCount;
		ColumnRes[] columns = rq.columns;
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
		
		for (int j = 0; j < rq.nbRow; j++) {
			for (int i = 0; i< columnCount; i++) {
				if (i!=0) {
					out.append('|');
				}
				String n = columns[i].values.get(j) + BLANK.substring(0, columns[i].max - columns[i].values.get(j).length());
				out.append(n);
			}
			out.append("\n");
		}
		return out.toString();
	}

	private ResultQuery executeQuery(Connection conn, String request) throws SQLException {
		ResultQuery rq = new ResultQuery();
		ResultSet executeQuery = conn.prepareStatement(request).executeQuery();
		rq.columnCount = executeQuery.getMetaData().getColumnCount();
		
		rq.columns = new ColumnRes[rq.columnCount];
		rq.nbRow = 0;
		
		for (int i = 1; i <= rq.columnCount; i++) {
			String columnClassName = executeQuery.getMetaData().getColumnClassName(i);
			
			String columnName = executeQuery.getMetaData().getColumnName(i);
			rq.columns[i-1] = createResColumn(columnClassName, columnName);
		}
		
		
		while (executeQuery.next()) {
			for (int i = 1; i <= rq.columnCount; i++) {
				rq.columns[i-1].add(executeQuery.getObject(i));
			}
			rq.nbRow ++;
		}
		return rq;
	}

	private String readInput(String fileName) throws FileNotFoundException, IOException {
		String result="";
		FileInputStream fis = new FileInputStream(fileName);
		try(BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fis))) {
			result = bufferedReader.lines().collect(Collectors.joining("\n"));
		}
		return result;
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
			mapper = o -> o.toString();
			break;
		}
		return new ColumnRes(columnName, o-> o!= null ? mapper.apply(o) : "NULL");
	}

	private void createPrices(Connection conn) throws Exception {
		/*conn.prepareStatement(
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
		conn.prepareStatement("INSERT INTO PRICE VALUES(6, 8)").execute();*/
		
		String readInput = readInput("priceAndDeals.sql");
		conn.prepareCall(readInput).execute();
		
	}

//	private void createDeals(Connection conn) throws SQLException {
//		conn.prepareStatement(
//				"CREATE TABLE DEAL ("
//				+ "  DEALID INT PRIMARY KEY,"
//				+ "  NAME VARCHAR"
//				+ ");"
//						).execute();
//		conn.prepareStatement("INSERT INTO DEAL VALUES(1, 'A')").execute();
//		conn.prepareStatement("INSERT INTO DEAL VALUES(2, 'B')").execute();
//		conn.prepareStatement("INSERT INTO DEAL VALUES(3, 'C')").execute();
//		conn.prepareStatement("INSERT INTO DEAL VALUES(4, 'D')").execute();
//		conn.prepareStatement("INSERT INTO DEAL VALUES(5, 'E')").execute();
//		conn.prepareStatement("INSERT INTO DEAL VALUES(6, 'F')").execute();
//	}
	
}
