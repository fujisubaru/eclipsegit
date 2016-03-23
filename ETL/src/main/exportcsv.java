package main;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * ユーザデータベースへのアクセステストクラス。
 */
public class exportcsv {

	private static Calendar        cal     = Calendar.getInstance();
	private static DateFormat      df      = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
	private static String edit_date = "";
    private static PrintWriter out ;

    private String csvfile1 = "c:\\export\\export.csv";
	/**
	 * テーブル名。
	 */
//	private static final String TABLE_NAME = "kyoten_m";

	/**
	 * テスト処理を実行します。
	 * @param args
	 */
	public static void main(String[] args) {
		exportcsv expcsv = new exportcsv();

		try{
//			PrintStream out = new PrintStream("c:\\debug.log");
//			System.setOut(out);
			// オブジェクトを生成
			expcsv.createOracle();
//			test.createMysql();

			// データ操作
//			test.execute(args);
			cal     = Calendar.getInstance();
			edit_date = (df.format(cal.getTime()));
			System.out.println("***** " + edit_date + " CSVデータ出力バッチ開始 *****");

//			test.executeTRUNCATE();

			expcsv.executeSelect1();
			cal     = Calendar.getInstance();
			edit_date = (df.format(cal.getTime()));
			System.out.println("***** " + edit_date + " CSVデータ出力バッチ終了 *****");
		}catch(Throwable t) {
			t.printStackTrace();
		}finally{
			// オブジェクトを破棄
			expcsv.closeOracle();
//			test.closeMySQL();
		}
	}

	/**
	 * Connectionオブジェクトを保持します。
	 */
	private Connection _connectionOracle;
	private Connection _connectionMySQL;

	/**
	 * Statementオブジェクトを保持します。
	 */
	private Statement _statementOracle;
	private Statement _statementMySQL;

	/**
	 * 構築します。
	 */
	public exportcsv() {
		_connectionOracle = null;
		_statementOracle = null;
		_connectionMySQL = null;
		_statementMySQL = null;
	}

	/**
	 * オブジェクトを生成します。
	 */
	public void createOracle()
		throws ClassNotFoundException, SQLException{
		// 下準備
//		Class.forName("com.mysql.jdbc.Driver");
		Class.forName("oracle.jdbc.OracleDriver");
//		_connection = DriverManager.getConnection("jdbc:mysql://10.3.129.1:3306/JAF?useUnicode=true&amp;characterEncoding=UTF-8", "partner21", "password");
		_connectionOracle = DriverManager.getConnection("jdbc:oracle:thin:@(DESCRIPTION=(LOAD_BALANCE=on)(FAILOVER=on)(ADDRESS_LIST=(ADDRESS=(protocol=tcp)(host=PP2PDB01)(port=1521))(ADDRESS=(protocol=tcp)(host=PP2PDB02)(port=1521))(ADDRESS=(protocol=tcp)(host=PP2PDB03)(port=1521)))(CONNECT_DATA=(SERVER=SHARED)(SERVICE_NAME=OPTD0000E.PP21_PRODUCT)))", "SUBARU0801", "cfP03Fc2e7ay");
		_statementOracle = _connectionOracle.createStatement();
	}

	public void createMysql()
			throws ClassNotFoundException, SQLException{
			// 下準備
			Class.forName("com.mysql.jdbc.Driver");
//			Class.forName("oracle.jdbc.OracleDriver");
			_connectionMySQL = DriverManager.getConnection("jdbc:mysql://10.3.129.1:3306/JAF?useUnicode=true&amp;characterEncoding=UTF-8", "partner21", "password");
//			_connectionOracle = DriverManager.getConnection("jdbc:oracle:thin:@(DESCRIPTION=(LOAD_BALANCE=on)(FAILOVER=on)(ADDRESS_LIST=(ADDRESS=(protocol=tcp)(host=PP2PDB01)(port=1521))(ADDRESS=(protocol=tcp)(host=PP2PDB02)(port=1521))(ADDRESS=(protocol=tcp)(host=PP2PDB03)(port=1521)))(CONNECT_DATA=(SERVER=SHARED)(SERVICE_NAME=OPTD0000E.PP21_PRODUCT)))", "SUBARU0801", "cfP03Fc2e7ay");
			_statementMySQL = _connectionMySQL.createStatement();
		}

	/**
	 * 各種オブジェクトを閉じます。
	 */
	public void closeOracle() {
		if(_statementOracle != null) {
			try{
				_statementOracle.close();
			}catch(SQLException e) {
				;
			}
			_statementOracle = null;
		}
		if(_connectionOracle != null) {
			try{
				_connectionOracle.close();
			}catch(SQLException e) {
				;
			}
			_connectionOracle = null;
		}
	}

	public void closeMySQL() {
		if(_statementMySQL != null) {
			try{
				_statementMySQL.close();
			}catch(SQLException e) {
				;
			}
			_statementMySQL = null;
		}
		if(_connectionMySQL != null) {
			try{
				_connectionMySQL.close();
			}catch(SQLException e) {
				;
			}
			_connectionMySQL = null;
		}
	}

	/**
	 * 実行します。
	 * @param args
	 * @throws SQLException
	 */
	public void execute(String[] args)
		throws SQLException {
//		String command = args[0];
//		if("select".equals(command)) {
			executeSelect1();
//		}else if("insert".equals(command)) {
//			executeInsert(args[1], args[2], args[3]);
//		}else if("update".equals(command)) {
//			executeUpdate(args[1], args[2], args[3]);
//		}else if("delete".equals(command)) {
//			executeDelete(args[1]);
//		}
	}

	/**
	 * SELECT処理を実行します。
	 */
	private void executeSelect1()
		throws SQLException{
		cal     = Calendar.getInstance();
		edit_date = (df.format(cal.getTime()));
		System.out.println("***** " + edit_date + " 顧客データCSVファイル出力開始 *****");
		ResultSet resultSetOracle = _statementOracle.executeQuery("SELECT PTAD1310.お客様コード, TRIM(BOTH '　' FROM PTAD1310.お客様名１) || '　' || TRIM(BOTH '　' FROM PTAD1310.お客様名２) AS お客様名, TRIM(BOTH '　' FROM PTAD1310.住所１) || TRIM(BOTH '　' FROM PTAD1310.住所２) || TRIM(BOTH '　' FROM PTAD1310.住所３) AS 住所,PTAD1310.性別, PTAD1310.生年月日, PTAD1310.担当拠点コード, TRIM(BOTH '　' FROM PTYM0130.拠点名) AS 担当拠点, PTAD1310.担当セールスコード, COALESCE(TRIM(BOTH '　' FROM PTYM0120.社員名),'　') AS 担当セールス FROM (PTAD1310 LEFT JOIN PTYM0130 ON PTAD1310.担当拠点コード = PTYM0130.拠点コード) LEFT JOIN  PTYM0120 ON PTAD1310.担当セールスコード = PTYM0120.社員コード WHERE PTAD1310.顧客削除日 = 0");
		try{
//			boolean br = resultSet.first();
//			if(br == false) {
//				return;
//			}
			try {
				out = new PrintWriter(csvfile1);
			} catch (FileNotFoundException e) {
				// TODO 自動生成された catch ブロック
				e.printStackTrace();
			}
			long RecordCount = 0;
			while(resultSetOracle.next()){
				String customer_code = resultSetOracle.getString("お客様コード");
				String customer_name = resultSetOracle.getString("お客様名");
				String shop_name = resultSetOracle.getString("担当拠点");
				String sales_name = resultSetOracle.getString("担当セールス");

				out.print("\"");out.print(customer_code);out.print("\",\"");
				out.print(customer_name);out.print("\",\"");
				out.print(shop_name);out.print("\",\"");
				out.print(sales_name);out.print("\"");out.print(System.getProperty("line.separator"));
//				int updateCount = _statementMySQL.executeUpdate("INSERT INTO customer_d (customer_code,customer_name,shop_name,sales_name) VALUES ('"+customer_code+"','"+customer_name+"','"+shop_name+"','"+sales_name+"')");

//				System.out.println("お客様コード: " + customer_code + ", お客様名: " + customer_name + ", 担当拠点: " + shop_name + ",担当セールス: " + sales_name);
//				System.out.println("***** " + edit_date + " データSELECT *****");
				RecordCount++;
			}
			out.close();
			cal     = Calendar.getInstance();
			edit_date = (df.format(cal.getTime()));
			System.out.println("***** " + edit_date + " 顧客データCSVファイル出力終了 出力件数=" + RecordCount + "件 *****");
		}finally{
			resultSetOracle.close();
		}
	}

	/**
	 * TRUNCATE処理を実行します。
	 * @param
	 */
	private void executeTRUNCATE()
		throws SQLException{
		// SQL文を発行
		_statementMySQL.executeUpdate("TRUNCATE TABLE customer_d");
		cal     = Calendar.getInstance();
		edit_date = (df.format(cal.getTime()));
		System.out.println("***** " + edit_date + " 顧客データ削除終了 *****");
	}

}
