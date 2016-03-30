package main;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Properties;
//import java.util.ResourceBundle;

/**
 * ユーザデータベースへのアクセステストクラス。
 */
public class exportcsv {

	private static Calendar        cal     = Calendar.getInstance();
	private static DateFormat      df      = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
	private static DateFormat      df2      = new SimpleDateFormat("yyyyMMdd");
	private static String edit_date = "";
	private static String today_date = "";
    private static PrintWriter out ;

    private static String csvfile1 = "";
    private static String csvfile2 = "";
    private static String csvfile3 = "";
    private static String csvfile4 = "";
    private static String csvfile5 = "";
    private static String csvfile6 = "";
    private static String csvfile7 = "";
    private static String csvfile8 = "";
    private static String csvfile9 = "";
    private static String csvfile10 = "";
    private static String csvfile11 = "";
    private static String csvfile12 = "";

    /**
	 * テーブル名。
	 */
//	private static final String TABLE_NAME = "kyoten_m";

	/**
	 * テスト処理を実行します。
	 * @param args
	 */
	public static void main(String[] args) {
		Properties properties = new Properties();
//	    ResourceBundle rb = ResourceBundle.getBundle("exportcsv.properties");
//	    csvfile1 = rb.getString("csvfile1");
		exportcsv expcsv = new exportcsv();

	    try{
	    	properties.load(new InputStreamReader(new FileInputStream("conf/exportcsv.properties"),"UTF-8"));
	    	csvfile1 = properties.getProperty("csvfile1");
	    	csvfile2 = properties.getProperty("csvfile2");
	    	csvfile3 = properties.getProperty("csvfile3");
	    	csvfile4 = properties.getProperty("csvfile4");
	    	csvfile5 = properties.getProperty("csvfile5");
	    	csvfile6 = properties.getProperty("csvfile6");
	    	csvfile7 = properties.getProperty("csvfile7");
	    	csvfile8 = properties.getProperty("csvfile8");
	    	csvfile9 = properties.getProperty("csvfile9");
	    	csvfile10 = properties.getProperty("csvfile10");
	    	csvfile11 = properties.getProperty("csvfile11");
	    	csvfile12 = properties.getProperty("csvfile12");
//			PrintStream out = new PrintStream("c:\\debug.log");
//			System.setOut(out);
			// オブジェクトを生成
			expcsv.createOracle();
			expcsv.createMysql();

			// データ操作
//			test.execute(args);
			cal     = Calendar.getInstance();
			today_date = (df2.format(cal.getTime()));

			cal     = Calendar.getInstance();
			edit_date = (df.format(cal.getTime()));
			System.out.println("***** " + edit_date + " CSVデータ出力バッチ開始 *****");

//			test.executeTRUNCATE();

			expcsv.executeSelect1();
			expcsv.executeSelect2();
			expcsv.executeSelect3();
			expcsv.executeSelect4();
			expcsv.executeSelect5();
			expcsv.executeSelect6();
			expcsv.executeSelect7();
			expcsv.executeSelect8();
			expcsv.executeSelect9();
			expcsv.executeSelect10();
			expcsv.executeSelect11();
			expcsv.executeSelect12();
			cal     = Calendar.getInstance();
			edit_date = (df.format(cal.getTime()));
			System.out.println("***** " + edit_date + " CSVデータ出力バッチ終了 *****");
		}catch(Throwable t) {
			t.printStackTrace();
		}finally{
			// オブジェクトを破棄
			expcsv.closeOracle();
			expcsv.closeMySQL();
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
			_connectionMySQL = DriverManager.getConnection("jdbc:mysql://10.3.129.1:3306/DWH?useUnicode=true&amp;characterEncoding=UTF-8", "partner21", "password");
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
//	public void execute(String[] args)
//		throws SQLException {
//		String command = args[0];
//		if("select".equals(command)) {
//			executeSelect1();
//		}else if("insert".equals(command)) {
//			executeInsert(args[1], args[2], args[3]);
//		}else if("update".equals(command)) {
//			executeUpdate(args[1], args[2], args[3]);
//		}else if("delete".equals(command)) {
//			executeDelete(args[1]);
//		}
//	}

	/**
	 * SELECT処理を実行します。
	 */
	//DIM_顧客台帳
	private void executeSelect1()
		throws SQLException{
		cal     = Calendar.getInstance();
		edit_date = (df.format(cal.getTime()));
		System.out.println("***** " + edit_date + " 顧客DIM CSVファイル出力開始 *****");
		String SQL;
		SQL = "SELECT PTAD1310.お客様コード, PTAD1310.作成年月日, PTAD1310.作成時間, PTAD1310.作成ＴＲＡＮ, PTAD1310.作成端末ＩＰ, PTAD1310.作成社員コード, PTAD1310.更新年月日, PTAD1310.更新時間, PTAD1310.更新ＴＲＡＮ, PTAD1310.更新端末ＩＰ, PTAD1310.更新社員コード, PTAD1310.お客様名１, PTAD1310.お客様名２, PTAD1310.フリガナ１, PTAD1310.フリガナ２, PTAD1310.お客様名編集区分, PTAD1310.検索フリガナ, PTAD1310.法人担当者名, PTAD1310.代表者名１, PTAD1310.代表者名２, PTAD1310.代表者名フリガナ１, PTAD1310.代表者名フリガナ２, PTAD1310.代表者名編集区分, PTAD1310.内線, PTAD1310.郵便番号, PTAD1310.住所１, PTAD1310.住所２, PTAD1310.住所３, PTAD1310.住所コード, PTAD1310.性別, PTAD1310.生年月日, PTAD1310.電話番号, PTAD1310.検索電話番号, PTAD1310.ＦＡＸ番号, PTAD1310.\"電話番号（携）\", PTAD1310.\"検索電話番号（携）\", PTAD1310.\"Ｅ－ＭＡＩＬ\", PTAD1310.顧客区分, PTAD1310.取引先識別区分, PTAD1310.取引先コード, PTAD1310.大口コード, PTAD1310.スバルカード有無, PTAD1310.セールスメモ, PTAD1310.セールス評価, PTAD1310.世帯コード, PTAD1310.ＮＴ区分, PTAD1310.職業, PTAD1310.勤務先名, PTAD1310.勤務先部署, PTAD1310.\"電話番号（勤）\", PTAD1310.勤務先内線, PTAD1310.社員区分, PTAD1310.担当拠点コード, PTAD1310.担当セールスコード, PTAD1310.最終接触日, PTAD1310.ＪＡＦ等加入, PTAD1310.ＤＭ区分, PTAD1310.嗜好メモ, PTAD1310.訪問メモ, PTAD1310.保有車両台数, PTAD1310.顧客新設日, PTAD1310.顧客新設拠点コード, PTAD1310.顧客新設社員コード, PTAD1310.入手元メモ, PTAD1310.顧客変更日, PTAD1310.顧客変更拠点コード, PTAD1310.顧客変更社員コード, PTAD1310.前特約店コード, PTAD1310.\"前拠点名（略）\", PTAD1310.\"前セールス名（略）\", PTAD1310.顧客削除日, PTAD1310.削除日, PTAD1310.削除理由, PTAD1310.紹介件数, PTAD1310.新車販売売上高, PTAD1310.中古車販売売上高, PTAD1310.サービス部品売上高, PTAD1310.保険売上高, PTAD1310.入庫回数, PTAD1310.新車販売粗利, PTAD1310.中古車販売粗利, PTAD1310.サービス部品粗利, PTAD1310.保険粗利, PTYM0130.拠点名, PTYM0130.拠点名略１, PTYM0130.拠点名略２, PTYM0130.郵便番号 AS 拠点郵便番号, PTYM0130.削除区分 AS 拠点削除区分, NVL(PTYM0120.社員名,'　') AS 社員氏名, NVL(PTYM0120.\"社員名（略）２\",'　') AS 社員名略, NVL(PTYM0120.削除区分,'0') AS 社員削除区分";
		SQL = SQL + " FROM PTYM0120 RIGHT JOIN (PTYM0130 RIGHT JOIN PTAD1310 ON PTYM0130.拠点コード = PTAD1310.担当拠点コード) ON PTYM0120.社員コード = PTAD1310.担当セールスコード WHERE 顧客削除日 = 0";
		ResultSet resultSetOracle = _statementOracle.executeQuery(SQL);
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
			//見出し出力
			out.print("\"お客様コード\",\"作成年月日\",\"作成時間\",\"作成ＴＲＡＮ\",\"作成端末ＩＰ\",\"作成社員コード\",\"更新年月日\",\"更新時間\",\"更新ＴＲＡＮ\",\"更新端末ＩＰ\",\"更新社員コード\",\"お客様名１\",\"お客様名２\",\"フリガナ１\",\"フリガナ２\",\"お客様名編集区分\",\"検索フリガナ\",\"法人担当者名\",\"代表者名１\",\"代表者名２\",\"代表者名フリガナ１\",\"代表者名フリガナ２\",\"代表者名編集区分\",\"内線\",\"郵便番号\",\"住所１\",\"住所２\",\"住所３\",\"住所コード\",\"性別\",\"生年月日\",\"電話番号\",\"検索電話番号\",\"ＦＡＸ番号\",\"電話番号（携）\",\"検索電話番号（携）\",\"Ｅ－ＭＡＩＬ\",\"顧客区分\",\"取引先識別区分\",\"取引先コード\",\"大口コード\",\"スバルカード有無\",\"セールスメモ\",\"セールス評価\",\"世帯コード\",\"ＮＴ区分\",\"職業\",\"勤務先名\",\"勤務先部署\",\"電話番号（勤）\",\"勤務先内線\",\"社員区分\",\"担当拠点コード\",\"担当セールスコード\",\"最終接触日\",\"ＪＡＦ等加入\",\"ＤＭ区分\",\"嗜好メモ\",\"訪問メモ\",\"保有車両台数\",\"顧客新設日\",\"顧客新設拠点コード\",\"顧客新設社員コード\",\"入手元メモ\",\"顧客変更日\",\"顧客変更拠点コード\",\"顧客変更社員コード\",\"前特約店コード\",\"前拠点名（略）\",\"前セールス名（略）\",\"顧客削除日\",\"削除日\",\"削除理由\",\"紹介件数\",\"新車販売売上高\",\"中古車販売売上高\",\"サービス部品売上高\",\"保険売上高\",\"入庫回数\",\"新車販売粗利\",\"中古車販売粗利\",\"サービス部品粗利\",\"保険粗利\",\"お客様経度緯度\",\"拠点名\",\"拠点名略１\",\"拠点名略２\",\"拠点郵便番号\",\"拠点削除区分\",\"拠点経度緯度\",\"社員名\",\"社員名（略）２\",\"社員削除区分\",\"今日\"");out.print(System.getProperty("line.separator"));

			long RecordCount = 0;
			while(resultSetOracle.next()){

				String customer_longitude_latitudeString = "";
				String shop_longitude_latitudeString = "";
				ResultSet resultSetMySQL = _statementMySQL.executeQuery("select longitude_latitude from longitude_latitude_m where postal_code =  '"+ resultSetOracle.getString("郵便番号") + "'");
				try{
					while(resultSetMySQL.next()){
						customer_longitude_latitudeString = resultSetMySQL.getString("longitude_latitude");
					}
				}finally{
					resultSetMySQL.close();
				}
				ResultSet resultSetMySQL2 = _statementMySQL.executeQuery("select longitude_latitude from longitude_latitude_m where postal_code =  '"+ resultSetOracle.getString("拠点郵便番号") + "'");
				try{
					while(resultSetMySQL2.next()){
						shop_longitude_latitudeString = resultSetMySQL2.getString("longitude_latitude");
					}
				}finally{
					resultSetMySQL2.close();
				}

				out.print("\"");out.print(resultSetOracle.getString("お客様コード"));out.print("\",\"");
				out.print(resultSetOracle.getInt("作成年月日"));out.print("\",\"");
				out.print(resultSetOracle.getInt("作成時間"));out.print("\",\"");
				out.print(resultSetOracle.getString("作成ＴＲＡＮ"));out.print("\",\"");
				out.print(resultSetOracle.getString("作成端末ＩＰ"));out.print("\",\"");
				out.print(resultSetOracle.getString("作成社員コード"));out.print("\",\"");
				out.print(resultSetOracle.getInt("更新年月日"));out.print("\",\"");
				out.print(resultSetOracle.getInt("更新時間"));out.print("\",\"");
				out.print(resultSetOracle.getString("更新ＴＲＡＮ"));out.print("\",\"");
				out.print(resultSetOracle.getString("更新端末ＩＰ"));out.print("\",\"");
				out.print(resultSetOracle.getString("更新社員コード"));out.print("\",\"");
				out.print(resultSetOracle.getString("お客様名１"));out.print("\",\"");
				out.print(resultSetOracle.getString("お客様名２"));out.print("\",\"");
				out.print(resultSetOracle.getString("フリガナ１"));out.print("\",\"");
				out.print(resultSetOracle.getString("フリガナ２"));out.print("\",\"");
				out.print(resultSetOracle.getString("お客様名編集区分"));out.print("\",\"");
				out.print(resultSetOracle.getString("検索フリガナ"));out.print("\",\"");
				out.print(resultSetOracle.getString("法人担当者名"));out.print("\",\"");
				out.print(resultSetOracle.getString("代表者名１"));out.print("\",\"");
				out.print(resultSetOracle.getString("代表者名２"));out.print("\",\"");
				out.print(resultSetOracle.getString("代表者名フリガナ１"));out.print("\",\"");
				out.print(resultSetOracle.getString("代表者名フリガナ２"));out.print("\",\"");
				out.print(resultSetOracle.getString("代表者名編集区分"));out.print("\",\"");
				out.print(resultSetOracle.getString("内線"));out.print("\",\"");
				out.print(resultSetOracle.getString("郵便番号"));out.print("\",\"");
				out.print(resultSetOracle.getString("住所１"));out.print("\",\"");
				out.print(resultSetOracle.getString("住所２"));out.print("\",\"");
				out.print(resultSetOracle.getString("住所３"));out.print("\",\"");
				out.print(resultSetOracle.getString("住所コード"));out.print("\",\"");
				out.print(resultSetOracle.getString("性別"));out.print("\",\"");
				out.print(resultSetOracle.getInt("生年月日"));out.print("\",\"");
				out.print(resultSetOracle.getString("電話番号"));out.print("\",\"");
				out.print(resultSetOracle.getString("検索電話番号"));out.print("\",\"");
				out.print(resultSetOracle.getString("ＦＡＸ番号"));out.print("\",\"");
				out.print(resultSetOracle.getString("電話番号（携）"));out.print("\",\"");
				out.print(resultSetOracle.getString("検索電話番号（携）"));out.print("\",\"");
				out.print(resultSetOracle.getString("Ｅ－ＭＡＩＬ"));out.print("\",\"");
				out.print(resultSetOracle.getString("顧客区分"));out.print("\",\"");
				out.print(resultSetOracle.getString("取引先識別区分"));out.print("\",\"");
				out.print(resultSetOracle.getString("取引先コード"));out.print("\",\"");
				out.print(resultSetOracle.getString("大口コード"));out.print("\",\"");
				out.print(resultSetOracle.getString("スバルカード有無"));out.print("\",\"");
				out.print(resultSetOracle.getString("セールスメモ"));out.print("\",\"");
				out.print(resultSetOracle.getString("セールス評価"));out.print("\",\"");
				out.print(resultSetOracle.getString("世帯コード"));out.print("\",\"");
				out.print(resultSetOracle.getString("ＮＴ区分"));out.print("\",\"");
				out.print(resultSetOracle.getString("職業"));out.print("\",\"");
				out.print(resultSetOracle.getString("勤務先名"));out.print("\",\"");
				out.print(resultSetOracle.getString("勤務先部署"));out.print("\",\"");
				out.print(resultSetOracle.getString("電話番号（勤）"));out.print("\",\"");
				out.print(resultSetOracle.getString("勤務先内線"));out.print("\",\"");
				out.print(resultSetOracle.getString("社員区分"));out.print("\",\"");
				out.print(resultSetOracle.getString("担当拠点コード"));out.print("\",\"");
				out.print(resultSetOracle.getString("担当セールスコード"));out.print("\",\"");
				out.print(resultSetOracle.getInt("最終接触日"));out.print("\",\"");
				out.print(resultSetOracle.getString("ＪＡＦ等加入"));out.print("\",\"");
				out.print(resultSetOracle.getString("ＤＭ区分"));out.print("\",\"");
				out.print(resultSetOracle.getString("嗜好メモ"));out.print("\",\"");
				out.print(resultSetOracle.getString("訪問メモ"));out.print("\",\"");
				out.print(resultSetOracle.getInt("保有車両台数"));out.print("\",\"");
				out.print(resultSetOracle.getInt("顧客新設日"));out.print("\",\"");
				out.print(resultSetOracle.getString("顧客新設拠点コード"));out.print("\",\"");
				out.print(resultSetOracle.getString("顧客新設社員コード"));out.print("\",\"");
				out.print(resultSetOracle.getString("入手元メモ"));out.print("\",\"");
				out.print(resultSetOracle.getInt("顧客変更日"));out.print("\",\"");
				out.print(resultSetOracle.getString("顧客変更拠点コード"));out.print("\",\"");
				out.print(resultSetOracle.getString("顧客変更社員コード"));out.print("\",\"");
				out.print(resultSetOracle.getString("前特約店コード"));out.print("\",\"");
				out.print(resultSetOracle.getString("前拠点名（略）"));out.print("\",\"");
				out.print(resultSetOracle.getString("前セールス名（略）"));out.print("\",\"");
				out.print(resultSetOracle.getInt("顧客削除日"));out.print("\",\"");
				out.print(resultSetOracle.getInt("削除日"));out.print("\",\"");
				out.print(resultSetOracle.getString("削除理由"));out.print("\",\"");
				out.print(resultSetOracle.getInt("紹介件数"));out.print("\",\"");
				out.print(resultSetOracle.getLong("新車販売売上高"));out.print("\",\"");
				out.print(resultSetOracle.getLong("中古車販売売上高"));out.print("\",\"");
				out.print(resultSetOracle.getLong("サービス部品売上高"));out.print("\",\"");
				out.print(resultSetOracle.getLong("保険売上高"));out.print("\",\"");
				out.print(resultSetOracle.getInt("入庫回数"));out.print("\",\"");
				out.print(resultSetOracle.getLong("新車販売粗利"));out.print("\",\"");
				out.print(resultSetOracle.getLong("中古車販売粗利"));out.print("\",\"");
				out.print(resultSetOracle.getLong("サービス部品粗利"));out.print("\",\"");
				out.print(resultSetOracle.getLong("保険粗利"));out.print("\",\"");
				out.print(customer_longitude_latitudeString);out.print("\",\"");
				out.print(resultSetOracle.getString("拠点名"));out.print("\",\"");
				out.print(resultSetOracle.getString("拠点名略１"));out.print("\",\"");
				out.print(resultSetOracle.getString("拠点名略２"));out.print("\",\"");
				out.print(resultSetOracle.getString("拠点郵便番号"));out.print("\",\"");
				out.print(resultSetOracle.getString("拠点削除区分"));out.print("\",\"");
				out.print(shop_longitude_latitudeString);out.print("\",\"");
				out.print(resultSetOracle.getString("社員氏名"));out.print("\",\"");
				out.print(resultSetOracle.getString("社員名略"));out.print("\",\"");
				out.print(resultSetOracle.getString("社員削除区分"));out.print("\",\"");
				out.print(today_date);out.print("\"");out.print(System.getProperty("line.separator"));
//				int updateCount = _statementMySQL.executeUpdate("INSERT INTO customer_d (customer_code,customer_name,shop_name,sales_name) VALUES ('"+customer_code+"','"+customer_name+"','"+shop_name+"','"+sales_name+"')");

//				System.out.println("お客様コード: " + customer_code + ", お客様名: " + customer_name + ", 担当拠点: " + shop_name + ",担当セールス: " + sales_name);
//				System.out.println("***** " + edit_date + " データSELECT *****");
				RecordCount++;
			}
			out.close();
			cal     = Calendar.getInstance();
			edit_date = (df.format(cal.getTime()));
			System.out.println("***** " + edit_date + " 顧客DIM CSVファイル出力終了 出力件数=" + RecordCount + "件 *****");
		}finally{
			resultSetOracle.close();
		}
	}

	//FACT_車両台帳
	private void executeSelect2()
		throws SQLException{
		cal     = Calendar.getInstance();
		edit_date = (df.format(cal.getTime()));
	    String lease_canceldate = "";
	    String lease_enddate = "";
	    String delivery_date = "";
	    String latestwarehousing_date = "";
		System.out.println("***** " + edit_date + " 車両FACT CSVファイル出力開始 *****");
		String SQL;
		SQL = "SELECT PTAD1320.車台番号, PTAD1320.作成年月日, PTAD1320.作成時間, PTAD1320.作成ＴＲＡＮ, PTAD1320.作成端末ＩＰ, PTAD1320.作成社員コード, PTAD1320.更新年月日, PTAD1320.更新時間, PTAD1320.更新ＴＲＡＮ, PTAD1320.更新端末ＩＰ, PTAD1320.更新社員コード, PTAD1320.\"登録番号－陸事\", PTAD1320.\"登録番号－車両区分\", PTAD1320.\"登録番号－カナ\", PTAD1320.\"登録番号－連番\", PTAD1320.登録年月日, PTAD1320.車検満了年月日, PTAD1320.初度登録年月, PTAD1320.用途, PTAD1320.\"自家用・事業用\", PTAD1320.車名コード, PTAD1320.車種コード, PTAD1320.\"型式（全銘柄）\", PTAD1320.ＯＰ, PTAD1320.仕様, PTAD1320.型式指定番号, PTAD1320.類別区分番号, PTAD1320.自動車種別コード, PTAD1320.車種名, PTAD1320.\"赤帽・一般\", PTAD1320.販売経路, PTAD1320.活動依頼区分, PTAD1320.業販店コード, PTAD1320.納車日, PTAD1320.お客様コード, PTAD1320.注文書番号, PTAD1320.\"新車・中古車区分\", PTAD1320.車両販売価格, PTAD1320.保証有無区分, PTAD1320.保証期限, PTAD1320.リースアップ日, PTAD1320.リース契約番号, PTAD1320.リース会社コード, PTAD1320.\"クレ－完済予定日\", PTAD1320.\"クレ－契約番号\", PTAD1320.\"クレ－会社コード\", PTAD1320.保険満期日, PTAD1320.証券番号, PTAD1320.証券枝番号, PTAD1320.保険会社コード, PTAD1320.整備担当拠点コード, PTAD1320.入庫時走行距離, PTAD1320.最新入庫日, PTAD1320.最新ご用命事項, PTAD1320.技術料会員レス率, PTAD1320.部品代会員レス率, PTAD1320.合計会員レス率, PTAD1320.推奨点検日, PTAD1320.推奨点検内容, PTAD1320.車両新設日, PTAD1320.車両新設拠点コード, PTAD1320.車両新設社員コード, PTAD1320.車両変更日, PTAD1320.車両変更拠点コード, PTAD1320.車両変更社員コード, PTAD1320.点検パックコード, PTAD1320.点検パック開始日, PTAD1320.点検パック終了日, PTAD1320.保証延長タイプ, PTAD1320.保証延長開始日, PTAD1320.保証延長終了日, PTAD1320.ＨＤ保証区分, PTAD1320.ＨＤ開始日, PTAD1320.ＨＤ終了日, PTAD1320.車両メモ, PTAD1320.\"陸事コード（５桁）\", NVL(PTMD7310.メンテメニュー区分,'  ') AS メンテ区分, NVL(PTMD7310.リース終了年月日,0) AS リース終了日, NVL(PTMD7310.リース解約年月日,0) AS リース解約日";
		SQL = SQL + " FROM PTAD1320 LEFT JOIN PTMD7310 ON PTAD1320.車台番号 = PTMD7310.車台番号";
		ResultSet resultSetOracle = _statementOracle.executeQuery(SQL);
		try{
//			boolean br = resultSet.first();
//			if(br == false) {
//				return;
//			}
			try {
				out = new PrintWriter(csvfile2);
			} catch (FileNotFoundException e) {
				// TODO 自動生成された catch ブロック
				e.printStackTrace();
			}
			//見出し出力
			out.print("\"車台番号\",\"作成年月日\",\"作成時間\",\"作成ＴＲＡＮ\",\"作成端末ＩＰ\",\"作成社員コード\",\"更新年月日\",\"更新時間\",\"更新ＴＲＡＮ\",\"更新端末ＩＰ\",\"更新社員コード\",\"登録番号－陸事\",\"登録番号－車両区分\",\"登録番号－カナ\",\"登録番号－連番\",\"登録年月日\",\"車検満了年月日\",\"初度登録年月\",\"用途\",\"自家用・事業用\",\"車名コード\",\"車種コード\",\"型式（全銘柄）\",\"ＯＰ\",\"仕様\",\"型式指定番号\",\"類別区分番号\",\"自動車種別コード\",\"車種名\",\"赤帽・一般\",\"販売経路\",\"活動依頼区分\",\"業販店コード\",\"納車日\",\"お客様コード\",\"注文書番号\",\"新車・中古車区分\",\"車両販売価格\",\"保証有無区分\",\"保証期限\",\"リースアップ日\",\"リース契約番号\",\"リース会社コード\",\"クレ－完済予定日\",\"クレ－契約番号\",\"クレ－会社コード\",\"保険満期日\",\"証券番号\",\"証券枝番号\",\"保険会社コード\",\"整備担当拠点コード\",\"入庫時走行距離\",\"最新入庫日\",\"最新ご用命事項\",\"技術料会員レス率\",\"部品代会員レス率\",\"合計会員レス率\",\"推奨点検日\",\"推奨点検内容\",\"車両新設日\",\"車両新設拠点コード\",\"車両新設社員コード\",\"車両変更日\",\"車両変更拠点コード\",\"車両変更社員コード\",\"点検パックコード\",\"点検パック開始日\",\"点検パック終了日\",\"保証延長タイプ\",\"保証延長開始日\",\"保証延長終了日\",\"ＨＤ保証区分\",\"ＨＤ開始日\",\"ＨＤ終了日\",\"車両メモ\",\"陸事コード（５桁）\",\"メンテメニュー区分\",\"リース終了年月日\",\"リース解約年月日\",\"今日\"");out.print(System.getProperty("line.separator"));

			long RecordCount = 0;
			while(resultSetOracle.next()){

				lease_enddate="";
				lease_canceldate="";
				delivery_date="";
				latestwarehousing_date="";
				if(resultSetOracle.getInt("リース終了日") != 0){
					lease_enddate = resultSetOracle.getString("リース終了日");
				}
				if(resultSetOracle.getInt("リース解約日") != 0){
					lease_canceldate = resultSetOracle.getString("リース解約日");
				}
				if(resultSetOracle.getInt("納車日") != 0){
					delivery_date = resultSetOracle.getString("納車日");
				}
				if(resultSetOracle.getInt("最新入庫日") != 0){
					latestwarehousing_date = resultSetOracle.getString("最新入庫日");
				}
				out.print("\"");out.print(resultSetOracle.getString("車台番号"));out.print("\",\"");
				out.print(resultSetOracle.getInt("作成年月日"));out.print("\",\"");
				out.print(resultSetOracle.getInt("作成時間"));out.print("\",\"");
				out.print(resultSetOracle.getString("作成ＴＲＡＮ"));out.print("\",\"");
				out.print(resultSetOracle.getString("作成端末ＩＰ"));out.print("\",\"");
				out.print(resultSetOracle.getString("作成社員コード"));out.print("\",\"");
				out.print(resultSetOracle.getInt("更新年月日"));out.print("\",\"");
				out.print(resultSetOracle.getInt("更新時間"));out.print("\",\"");
				out.print(resultSetOracle.getString("更新ＴＲＡＮ"));out.print("\",\"");
				out.print(resultSetOracle.getString("更新端末ＩＰ"));out.print("\",\"");
				out.print(resultSetOracle.getString("更新社員コード"));out.print("\",\"");
				out.print(resultSetOracle.getString("登録番号－陸事"));out.print("\",\"");
				out.print(resultSetOracle.getString("登録番号－車両区分"));out.print("\",\"");
				out.print(resultSetOracle.getString("登録番号－カナ"));out.print("\",\"");
				out.print(resultSetOracle.getString("登録番号－連番"));out.print("\",\"");
				out.print(resultSetOracle.getInt("登録年月日"));out.print("\",\"");
				out.print(resultSetOracle.getInt("車検満了年月日"));out.print("\",\"");
				out.print(resultSetOracle.getInt("初度登録年月"));out.print("\",\"");
				out.print(resultSetOracle.getString("用途"));out.print("\",\"");
				out.print(resultSetOracle.getString("自家用・事業用"));out.print("\",\"");
				out.print(resultSetOracle.getString("車名コード"));out.print("\",\"");
				out.print(resultSetOracle.getString("車種コード"));out.print("\",\"");
				out.print(resultSetOracle.getString("型式（全銘柄）"));out.print("\",\"");
				out.print(resultSetOracle.getString("ＯＰ"));out.print("\",\"");
				out.print(resultSetOracle.getString("仕様"));out.print("\",\"");
				out.print(resultSetOracle.getString("型式指定番号"));out.print("\",\"");
				out.print(resultSetOracle.getString("類別区分番号"));out.print("\",\"");
				out.print(resultSetOracle.getString("自動車種別コード"));out.print("\",\"");
				out.print(resultSetOracle.getString("車種名"));out.print("\",\"");
				out.print(resultSetOracle.getString("赤帽・一般"));out.print("\",\"");
				out.print(resultSetOracle.getString("販売経路"));out.print("\",\"");
				out.print(resultSetOracle.getString("活動依頼区分"));out.print("\",\"");
				out.print(resultSetOracle.getString("業販店コード"));out.print("\",\"");
				out.print(delivery_date);out.print("\",\"");
				out.print(resultSetOracle.getString("お客様コード"));out.print("\",\"");
				out.print(resultSetOracle.getString("注文書番号"));out.print("\",\"");
				out.print(resultSetOracle.getString("新車・中古車区分"));out.print("\",\"");
				out.print(resultSetOracle.getInt("車両販売価格"));out.print("\",\"");
				out.print(resultSetOracle.getString("保証有無区分"));out.print("\",\"");
				out.print(resultSetOracle.getInt("保証期限"));out.print("\",\"");
				out.print(resultSetOracle.getInt("リースアップ日"));out.print("\",\"");
				out.print(resultSetOracle.getString("リース契約番号"));out.print("\",\"");
				out.print(resultSetOracle.getString("リース会社コード"));out.print("\",\"");
				out.print(resultSetOracle.getInt("クレ－完済予定日"));out.print("\",\"");
				out.print(resultSetOracle.getString("クレ－契約番号"));out.print("\",\"");
				out.print(resultSetOracle.getString("クレ－会社コード"));out.print("\",\"");
				out.print(resultSetOracle.getInt("保険満期日"));out.print("\",\"");
				out.print(resultSetOracle.getString("証券番号"));out.print("\",\"");
				out.print(resultSetOracle.getString("証券枝番号"));out.print("\",\"");
				out.print(resultSetOracle.getString("保険会社コード"));out.print("\",\"");
				out.print(resultSetOracle.getString("整備担当拠点コード"));out.print("\",\"");
				out.print(resultSetOracle.getInt("入庫時走行距離"));out.print("\",\"");
				out.print(latestwarehousing_date);out.print("\",\"");
				out.print(resultSetOracle.getString("最新ご用命事項"));out.print("\",\"");
				out.print(resultSetOracle.getInt("技術料会員レス率"));out.print("\",\"");
				out.print(resultSetOracle.getInt("部品代会員レス率"));out.print("\",\"");
				out.print(resultSetOracle.getInt("合計会員レス率"));out.print("\",\"");
				out.print(resultSetOracle.getInt("推奨点検日"));out.print("\",\"");
				out.print(resultSetOracle.getString("推奨点検内容"));out.print("\",\"");
				out.print(resultSetOracle.getInt("車両新設日"));out.print("\",\"");
				out.print(resultSetOracle.getString("車両新設拠点コード"));out.print("\",\"");
				out.print(resultSetOracle.getString("車両新設社員コード"));out.print("\",\"");
				out.print(resultSetOracle.getInt("車両変更日"));out.print("\",\"");
				out.print(resultSetOracle.getString("車両変更拠点コード"));out.print("\",\"");
				out.print(resultSetOracle.getString("車両変更社員コード"));out.print("\",\"");
				out.print(resultSetOracle.getString("点検パックコード"));out.print("\",\"");
				out.print(resultSetOracle.getInt("点検パック開始日"));out.print("\",\"");
				out.print(resultSetOracle.getInt("点検パック終了日"));out.print("\",\"");
				out.print(resultSetOracle.getString("保証延長タイプ"));out.print("\",\"");
				out.print(resultSetOracle.getInt("保証延長開始日"));out.print("\",\"");
				out.print(resultSetOracle.getInt("保証延長終了日"));out.print("\",\"");
				out.print(resultSetOracle.getString("ＨＤ保証区分"));out.print("\",\"");
				out.print(resultSetOracle.getInt("ＨＤ開始日"));out.print("\",\"");
				out.print(resultSetOracle.getInt("ＨＤ終了日"));out.print("\",\"");
				out.print(resultSetOracle.getString("車両メモ"));out.print("\",\"");
				out.print(resultSetOracle.getString("陸事コード（５桁）"));out.print("\",\"");
				out.print(resultSetOracle.getString("メンテ区分"));out.print("\",\"");
				out.print(lease_enddate);out.print("\",\"");
				out.print(lease_canceldate);out.print("\",\"");
				out.print(today_date);out.print("\"");out.print(System.getProperty("line.separator"));
//				int updateCount = _statementMySQL.executeUpdate("INSERT INTO customer_d (customer_code,customer_name,shop_name,sales_name) VALUES ('"+customer_code+"','"+customer_name+"','"+shop_name+"','"+sales_name+"')");

//				System.out.println("お客様コード: " + customer_code + ", お客様名: " + customer_name + ", 担当拠点: " + shop_name + ",担当セールス: " + sales_name);
//				System.out.println("***** " + edit_date + " データSELECT *****");
				RecordCount++;
			}
			out.close();
			cal     = Calendar.getInstance();
			edit_date = (df.format(cal.getTime()));
			System.out.println("***** " + edit_date + " 車両FACT CSVファイル出力終了 出力件数=" + RecordCount + "件 *****");
		}finally{
			resultSetOracle.close();
		}
	}

	//DIM_リース契約台帳
	private void executeSelect3()
		throws SQLException{
		cal     = Calendar.getInstance();
		edit_date = (df.format(cal.getTime()));
		System.out.println("***** " + edit_date + " リース契約DIM CSVファイル出力開始 *****");
		String SQL;
		String lease_enddate="";
		String lease_canceldate="";
		SQL = "SELECT PTMD7310.リース管理番号, PTMD7310.作成年月日, PTMD7310.作成時間, PTMD7310.作成ＴＲＡＮ, PTMD7310.作成端末ＩＰ, PTMD7310.作成社員コード, PTMD7310.更新年月日, PTMD7310.更新時間, PTMD7310.更新ＴＲＡＮ, PTMD7310.更新端末ＩＰ, PTMD7310.更新社員コード, PTMD7310.注文書番号, PTMD7310.車種コード, PTMD7310.自銘他銘区分, PTMD7310.小型軽区分, PTMD7310.型式, PTMD7310.仕様, PTMD7310.ＯＰ, PTMD7310.車台番号, PTMD7310.登録年月日, PTMD7310.登録番号, PTMD7310.初度登録年月, PTMD7310.陸事名, PTMD7310.取引先コード, PTMD7310.取引先名称, PTMD7310.お客様コード, PTMD7310.お客様名称, PTMD7310.請求先コード, PTMD7310.請求先名称, PTMD7310.使用の本拠地, PTMD7310.納入先名称, PTMD7310.担当拠点コード, PTMD7310.担当拠点, PTMD7310.担当社員コード, PTMD7310.担当セールス, PTMD7310.契約形態区分, PTMD7310.契約形態, PTMD7310.管理大別区分, PTMD7310.管理大別, PTMD7310.リース見積番号, PTMD7310.リース契約番号, PTMD7310.旧契約番号, PTMD7310.ＳＦＣメニュー区分, PTMD7310.ＳＦＣメニュー, PTMD7310.メンテメニュー区分, PTMD7310.メンテメニュー, PTMD7310.自動車保険付保区分, PTMD7310.自動車保険付保, PTMD7310.月額リース料, PTMD7310.リース期間, PTMD7310.請求回数, PTMD7310.請求開始年月, PTMD7310.売上計上年月, PTMD7310.リース開始年月日, PTMD7310.リース終了年月日, PTMD7310.リース解約年月日, PTMD7310.車両残存価格, PTMD7310.原状復帰精算金, PTMD7310.月間契約走行距離, PTMD7310.フォロー結果区分, PTMD7310.フォロー結果, PTMD7310.中古車入庫年月日, PTMD7310.中古車受入査定価格, PTMD7310.残存価格差, PTMD7310.特記事項 FROM PTMD7310";
		ResultSet resultSetOracle = _statementOracle.executeQuery(SQL);
		try{
//			boolean br = resultSet.first();
//			if(br == false) {
//				return;
//			}
			try {
				out = new PrintWriter(csvfile3);
			} catch (FileNotFoundException e) {
				// TODO 自動生成された catch ブロック
				e.printStackTrace();
			}
			//見出し出力
			out.print("\"リース管理番号\",\"作成年月日\",\"作成時間\",\"作成ＴＲＡＮ\",\"作成端末ＩＰ\",\"作成社員コード\",\"更新年月日\",\"更新時間\",\"更新ＴＲＡＮ\",\"更新端末ＩＰ\",\"更新社員コード\",\"注文書番号\",\"車種コード\",\"自銘他銘区分\",\"小型軽区分\",\"型式\",\"仕様\",\"ＯＰ\",\"車台番号\",\"登録年月日\",\"登録番号\",\"初度登録年月\",\"陸事名\",\"取引先コード\",\"取引先名称\",\"お客様コード\",\"お客様名称\",\"請求先コード\",\"請求先名称\",\"使用の本拠地\",\"納入先名称\",\"担当拠点コード\",\"担当拠点\",\"担当社員コード\",\"担当セールス\",\"契約形態区分\",\"契約形態\",\"管理大別区分\",\"管理大別\",\"リース見積番号\",\"リース契約番号\",\"旧契約番号\",\"ＳＦＣメニュー区分\",\"ＳＦＣメニュー\",\"メンテメニュー区分\",\"メンテメニュー\",\"自動車保険付保区分\",\"自動車保険付保\",\"月額リース料\",\"リース期間\",\"請求回数\",\"請求開始年月\",\"売上計上年月\",\"リース開始年月日\",\"リース終了年月日\",\"リース解約年月日\",\"車両残存価格\",\"原状復帰精算金\",\"月間契約走行距離\",\"フォロー結果区分\",\"フォロー結果\",\"中古車入庫年月日\",\"中古車受入査定価格\",\"残存価格差\",\"特記事項\",\"今日\"");out.print(System.getProperty("line.separator"));

			long RecordCount = 0;
			while(resultSetOracle.next()){

				lease_enddate="";
				lease_canceldate="";
				if(resultSetOracle.getInt("リース終了年月日") != 0){
					lease_enddate = resultSetOracle.getString("リース終了年月日");
				}
				if(resultSetOracle.getInt("リース解約年月日") != 0){
					lease_canceldate = resultSetOracle.getString("リース解約年月日");
				}
				out.print("\"");out.print(resultSetOracle.getString("リース管理番号"));out.print("\",\"");
				out.print(resultSetOracle.getInt("作成年月日"));out.print("\",\"");
				out.print(resultSetOracle.getInt("作成時間"));out.print("\",\"");
				out.print(resultSetOracle.getString("作成ＴＲＡＮ"));out.print("\",\"");
				out.print(resultSetOracle.getString("作成端末ＩＰ"));out.print("\",\"");
				out.print(resultSetOracle.getString("作成社員コード"));out.print("\",\"");
				out.print(resultSetOracle.getInt("更新年月日"));out.print("\",\"");
				out.print(resultSetOracle.getInt("更新時間"));out.print("\",\"");
				out.print(resultSetOracle.getString("更新ＴＲＡＮ"));out.print("\",\"");
				out.print(resultSetOracle.getString("更新端末ＩＰ"));out.print("\",\"");
				out.print(resultSetOracle.getString("更新社員コード"));out.print("\",\"");
				out.print(resultSetOracle.getString("注文書番号"));out.print("\",\"");
				out.print(resultSetOracle.getString("車種コード"));out.print("\",\"");
				out.print(resultSetOracle.getString("自銘他銘区分"));out.print("\",\"");
				out.print(resultSetOracle.getString("小型軽区分"));out.print("\",\"");
				out.print(resultSetOracle.getString("型式"));out.print("\",\"");
				out.print(resultSetOracle.getString("仕様"));out.print("\",\"");
				out.print(resultSetOracle.getString("ＯＰ"));out.print("\",\"");
				out.print(resultSetOracle.getString("車台番号"));out.print("\",\"");
				out.print(resultSetOracle.getInt("登録年月日"));out.print("\",\"");
				out.print(resultSetOracle.getString("登録番号"));out.print("\",\"");
				out.print(resultSetOracle.getInt("初度登録年月"));out.print("\",\"");
				out.print(resultSetOracle.getString("陸事名"));out.print("\",\"");
				out.print(resultSetOracle.getString("取引先コード"));out.print("\",\"");
				out.print(resultSetOracle.getString("取引先名称"));out.print("\",\"");
				out.print(resultSetOracle.getString("お客様コード"));out.print("\",\"");
				out.print(resultSetOracle.getString("お客様名称"));out.print("\",\"");
				out.print(resultSetOracle.getString("請求先コード"));out.print("\",\"");
				out.print(resultSetOracle.getString("請求先名称"));out.print("\",\"");
				out.print(resultSetOracle.getString("使用の本拠地"));out.print("\",\"");
				out.print(resultSetOracle.getString("納入先名称"));out.print("\",\"");
				out.print(resultSetOracle.getString("担当拠点コード"));out.print("\",\"");
				out.print(resultSetOracle.getString("担当拠点"));out.print("\",\"");
				out.print(resultSetOracle.getString("担当社員コード"));out.print("\",\"");
				out.print(resultSetOracle.getString("担当セールス"));out.print("\",\"");
				out.print(resultSetOracle.getString("契約形態区分"));out.print("\",\"");
				out.print(resultSetOracle.getString("契約形態"));out.print("\",\"");
				out.print(resultSetOracle.getString("管理大別区分"));out.print("\",\"");
				out.print(resultSetOracle.getString("管理大別"));out.print("\",\"");
				out.print(resultSetOracle.getString("リース見積番号"));out.print("\",\"");
				out.print(resultSetOracle.getString("リース契約番号"));out.print("\",\"");
				out.print(resultSetOracle.getString("旧契約番号"));out.print("\",\"");
				out.print(resultSetOracle.getString("ＳＦＣメニュー区分"));out.print("\",\"");
				out.print(resultSetOracle.getString("ＳＦＣメニュー"));out.print("\",\"");
				out.print(resultSetOracle.getString("メンテメニュー区分"));out.print("\",\"");
				out.print(resultSetOracle.getString("メンテメニュー"));out.print("\",\"");
				out.print(resultSetOracle.getString("自動車保険付保区分"));out.print("\",\"");
				out.print(resultSetOracle.getString("自動車保険付保"));out.print("\",\"");
				out.print(resultSetOracle.getInt("月額リース料"));out.print("\",\"");
				out.print(resultSetOracle.getInt("リース期間"));out.print("\",\"");
				out.print(resultSetOracle.getInt("請求回数"));out.print("\",\"");
				out.print(resultSetOracle.getInt("請求開始年月"));out.print("\",\"");
				out.print(resultSetOracle.getInt("売上計上年月"));out.print("\",\"");
				out.print(resultSetOracle.getInt("リース開始年月日"));out.print("\",\"");
				out.print(lease_enddate);out.print("\",\"");
				out.print(lease_canceldate);out.print("\",\"");
				out.print(resultSetOracle.getInt("車両残存価格"));out.print("\",\"");
				out.print(resultSetOracle.getInt("原状復帰精算金"));out.print("\",\"");
				out.print(resultSetOracle.getInt("月間契約走行距離"));out.print("\",\"");
				out.print(resultSetOracle.getString("フォロー結果区分"));out.print("\",\"");
				out.print(resultSetOracle.getString("フォロー結果"));out.print("\",\"");
				out.print(resultSetOracle.getInt("中古車入庫年月日"));out.print("\",\"");
				out.print(resultSetOracle.getInt("中古車受入査定価格"));out.print("\",\"");
				out.print(resultSetOracle.getInt("残存価格差"));out.print("\",\"");
				out.print(resultSetOracle.getString("特記事項"));out.print("\",\"");
				out.print(today_date);out.print("\"");out.print(System.getProperty("line.separator"));
//				int updateCount = _statementMySQL.executeUpdate("INSERT INTO customer_d (customer_code,customer_name,shop_name,sales_name) VALUES ('"+customer_code+"','"+customer_name+"','"+shop_name+"','"+sales_name+"')");

//				System.out.println("お客様コード: " + customer_code + ", お客様名: " + customer_name + ", 担当拠点: " + shop_name + ",担当セールス: " + sales_name);
//				System.out.println("***** " + edit_date + " データSELECT *****");
				RecordCount++;
			}
			out.close();
			cal     = Calendar.getInstance();
			edit_date = (df.format(cal.getTime()));
			System.out.println("***** " + edit_date + " リース契約DIM CSVファイル出力終了 出力件数=" + RecordCount + "件 *****");
		}finally{
			resultSetOracle.close();
		}
	}

	//DIM_新車型式マスタ
	private void executeSelect4()
		throws SQLException{
		cal     = Calendar.getInstance();
		edit_date = (df.format(cal.getTime()));
		System.out.println("***** " + edit_date + " 新車型式DIM CSVファイル出力開始 *****");
		String SQL;
		SQL = "SELECT PTOM2030.型式, PTOM2030.ＯＰ, PTOM2030.仕様, PTOM2030.作成年月日, PTOM2030.作成時間, PTOM2030.作成ＴＲＡＮ, PTOM2030.作成端末ＩＰ, PTOM2030.作成社員コード, PTOM2030.更新年月日, PTOM2030.更新時間, PTOM2030.更新ＴＲＡＮ, PTOM2030.更新端末ＩＰ, PTOM2030.更新社員コード, PTOM2030.車名コード, PTOM2030.車種コード, PTOM2030.シリーズ, PTOM2030.\"駆動－商談\", PTOM2030.\"排気量－商談\", PTOM2030.\"グレード－商談\", PTOM2030.シフト名, PTOM2030.標準車両本体価格, PTOM2030.車両本体価格, PTOM2030.仕切価格, PTOM2030.調整原価, PTOM2030.基本整備費, PTOM2030.\"仕様名（外装色）\", PTOM2030.\"仕様名（内装色）\", PTOM2030.ベースキット番号, PTOM2030.特別色加算額, PTOM2030.取得税識別区分, PTOM2030.自動車税管理番号, PTOM2030.重量税管理番号, PTOM2030.自賠自動車種別, PTOM2030.車種名, PTOM2030.車種名―商談, PTOM2030.主標準装備名１, PTOM2030.主標準装備名２, PTOM2030.主標準装備名３, PTOM2030.主標準装備名４, PTOM2030.主標準装備名５, PTOM2030.主標準装備名６, PTOM2030.主標準装備名７, PTOM2030.主標準装備名８, PTOM2030.主標準装備名９, PTOM2030.主標準装備名１０, PTOM2030.寒冷地識別区分 FROM PTOM2030";
		ResultSet resultSetOracle = _statementOracle.executeQuery(SQL);
		try{
//			boolean br = resultSet.first();
//			if(br == false) {
//				return;
//			}
			try {
				out = new PrintWriter(csvfile4);
			} catch (FileNotFoundException e) {
				// TODO 自動生成された catch ブロック
				e.printStackTrace();
			}
			//見出し出力
			out.print("\"型式\",\"ＯＰ\",\"仕様\",\"作成年月日\",\"作成時間\",\"作成ＴＲＡＮ\",\"作成端末ＩＰ\",\"作成社員コード\",\"更新年月日\",\"更新時間\",\"更新ＴＲＡＮ\",\"更新端末ＩＰ\",\"更新社員コード\",\"車名コード\",\"車種コード\",\"シリーズ\",\"駆動－商談\",\"排気量－商談\",\"グレード－商談\",\"シフト名\",\"標準車両本体価格\",\"車両本体価格\",\"仕切価格\",\"調整原価\",\"基本整備費\",\"仕様名（外装色）\",\"仕様名（内装色）\",\"ベースキット番号\",\"特別色加算額\",\"取得税識別区分\",\"自動車税管理番号\",\"重量税管理番号\",\"自賠自動車種別\",\"車種名\",\"車種名―商談\",\"主標準装備名１\",\"主標準装備名２\",\"主標準装備名３\",\"主標準装備名４\",\"主標準装備名５\",\"主標準装備名６\",\"主標準装備名７\",\"主標準装備名８\",\"主標準装備名９\",\"主標準装備名１０\",\"寒冷地識別区分\"");out.print(System.getProperty("line.separator"));

			long RecordCount = 0;
			while(resultSetOracle.next()){

				out.print("\"");out.print(resultSetOracle.getString("型式"));out.print("\",\"");
				out.print(resultSetOracle.getString("ＯＰ"));out.print("\",\"");
				out.print(resultSetOracle.getString("仕様"));out.print("\",\"");
				out.print(resultSetOracle.getInt("作成年月日"));out.print("\",\"");
				out.print(resultSetOracle.getInt("作成時間"));out.print("\",\"");
				out.print(resultSetOracle.getString("作成ＴＲＡＮ"));out.print("\",\"");
				out.print(resultSetOracle.getString("作成端末ＩＰ"));out.print("\",\"");
				out.print(resultSetOracle.getString("作成社員コード"));out.print("\",\"");
				out.print(resultSetOracle.getInt("更新年月日"));out.print("\",\"");
				out.print(resultSetOracle.getInt("更新時間"));out.print("\",\"");
				out.print(resultSetOracle.getString("更新ＴＲＡＮ"));out.print("\",\"");
				out.print(resultSetOracle.getString("更新端末ＩＰ"));out.print("\",\"");
				out.print(resultSetOracle.getString("更新社員コード"));out.print("\",\"");
				out.print(resultSetOracle.getString("車名コード"));out.print("\",\"");
				out.print(resultSetOracle.getString("車種コード"));out.print("\",\"");
				out.print(resultSetOracle.getString("シリーズ"));out.print("\",\"");
				out.print(resultSetOracle.getString("駆動－商談"));out.print("\",\"");
				out.print(resultSetOracle.getString("排気量－商談"));out.print("\",\"");
				out.print(resultSetOracle.getString("グレード－商談"));out.print("\",\"");
				out.print(resultSetOracle.getString("シフト名"));out.print("\",\"");
				out.print(resultSetOracle.getInt("標準車両本体価格"));out.print("\",\"");
				out.print(resultSetOracle.getInt("車両本体価格"));out.print("\",\"");
				out.print(resultSetOracle.getInt("仕切価格"));out.print("\",\"");
				out.print(resultSetOracle.getInt("調整原価"));out.print("\",\"");
				out.print(resultSetOracle.getInt("基本整備費"));out.print("\",\"");
				out.print(resultSetOracle.getString("仕様名（外装色）"));out.print("\",\"");
				out.print(resultSetOracle.getString("仕様名（内装色）"));out.print("\",\"");
				out.print(resultSetOracle.getString("ベースキット番号"));out.print("\",\"");
				out.print(resultSetOracle.getInt("特別色加算額"));out.print("\",\"");
				out.print(resultSetOracle.getString("取得税識別区分"));out.print("\",\"");
				out.print(resultSetOracle.getString("自動車税管理番号"));out.print("\",\"");
				out.print(resultSetOracle.getString("重量税管理番号"));out.print("\",\"");
				out.print(resultSetOracle.getString("自賠自動車種別"));out.print("\",\"");
				out.print(resultSetOracle.getString("車種名"));out.print("\",\"");
				out.print(resultSetOracle.getString("車種名―商談"));out.print("\",\"");
				out.print(resultSetOracle.getString("主標準装備名１"));out.print("\",\"");
				out.print(resultSetOracle.getString("主標準装備名２"));out.print("\",\"");
				out.print(resultSetOracle.getString("主標準装備名３"));out.print("\",\"");
				out.print(resultSetOracle.getString("主標準装備名４"));out.print("\",\"");
				out.print(resultSetOracle.getString("主標準装備名５"));out.print("\",\"");
				out.print(resultSetOracle.getString("主標準装備名６"));out.print("\",\"");
				out.print(resultSetOracle.getString("主標準装備名７"));out.print("\",\"");
				out.print(resultSetOracle.getString("主標準装備名８"));out.print("\",\"");
				out.print(resultSetOracle.getString("主標準装備名９"));out.print("\",\"");
				out.print(resultSetOracle.getString("主標準装備名１０"));out.print("\",\"");
				out.print(resultSetOracle.getString("寒冷地識別区分"));out.print("\"");out.print(System.getProperty("line.separator"));
//				int updateCount = _statementMySQL.executeUpdate("INSERT INTO customer_d (customer_code,customer_name,shop_name,sales_name) VALUES ('"+customer_code+"','"+customer_name+"','"+shop_name+"','"+sales_name+"')");

//				System.out.println("お客様コード: " + customer_code + ", お客様名: " + customer_name + ", 担当拠点: " + shop_name + ",担当セールス: " + sales_name);
//				System.out.println("***** " + edit_date + " データSELECT *****");
				RecordCount++;
			}
			out.close();
			cal     = Calendar.getInstance();
			edit_date = (df.format(cal.getTime()));
			System.out.println("***** " + edit_date + " 新車型式DIM CSVファイル出力終了 出力件数=" + RecordCount + "件 *****");
		}finally{
			resultSetOracle.close();
		}
	}

	//DIM_新車車種マスタ
	private void executeSelect5()
		throws SQLException{
		cal     = Calendar.getInstance();
		edit_date = (df.format(cal.getTime()));
		System.out.println("***** " + edit_date + " 新車車種DIM CSVファイル出力開始 *****");
		String SQL;
		SQL = "SELECT PTOM2110.車種コード, PTOM2110.作成年月日, PTOM2110.作成時間, PTOM2110.作成ＴＲＡＮ, PTOM2110.作成端末ＩＰ, PTOM2110.作成社員コード, PTOM2110.更新年月日, PTOM2110.更新時間, PTOM2110.更新ＴＲＡＮ, PTOM2110.更新端末ＩＰ, PTOM2110.更新社員コード, PTOM2110.車種名, PTOM2110.車名コード, PTOM2110.型式上２桁１, PTOM2110.年改番号１, PTOM2110.表示名１, PTOM2110.型式上２桁２, PTOM2110.年改番号２, PTOM2110.表示名２, PTOM2110.基本整備費, PTOM2110.電子完検適用開始日, PTOM2110.電子完検適用終了日 FROM PTOM2110";
		ResultSet resultSetOracle = _statementOracle.executeQuery(SQL);
		try{
//			boolean br = resultSet.first();
//			if(br == false) {
//				return;
//			}
			try {
				out = new PrintWriter(csvfile5);
			} catch (FileNotFoundException e) {
				// TODO 自動生成された catch ブロック
				e.printStackTrace();
			}
			//見出し出力
			out.print("\"車種コード\",\"作成年月日\",\"作成時間\",\"作成ＴＲＡＮ\",\"作成端末ＩＰ\",\"作成社員コード\",\"更新年月日\",\"更新時間\",\"更新ＴＲＡＮ\",\"更新端末ＩＰ\",\"更新社員コード\",\"車種名\",\"車名コード\",\"型式上２桁１\",\"年改番号１\",\"表示名１\",\"型式上２桁２\",\"年改番号２\",\"表示名２\",\"基本整備費\",\"電子完検適用開始日\",\"電子完検適用終了日\"");out.print(System.getProperty("line.separator"));

			long RecordCount = 0;
			while(resultSetOracle.next()){

				out.print("\"");out.print(resultSetOracle.getString("車種コード"));out.print("\",\"");
				out.print(resultSetOracle.getInt("作成年月日"));out.print("\",\"");
				out.print(resultSetOracle.getInt("作成時間"));out.print("\",\"");
				out.print(resultSetOracle.getString("作成ＴＲＡＮ"));out.print("\",\"");
				out.print(resultSetOracle.getString("作成端末ＩＰ"));out.print("\",\"");
				out.print(resultSetOracle.getString("作成社員コード"));out.print("\",\"");
				out.print(resultSetOracle.getInt("更新年月日"));out.print("\",\"");
				out.print(resultSetOracle.getInt("更新時間"));out.print("\",\"");
				out.print(resultSetOracle.getString("更新ＴＲＡＮ"));out.print("\",\"");
				out.print(resultSetOracle.getString("更新端末ＩＰ"));out.print("\",\"");
				out.print(resultSetOracle.getString("更新社員コード"));out.print("\",\"");
				out.print(resultSetOracle.getString("車種名"));out.print("\",\"");
				out.print(resultSetOracle.getString("車名コード"));out.print("\",\"");
				out.print(resultSetOracle.getString("型式上２桁１"));out.print("\",\"");
				out.print(resultSetOracle.getString("年改番号１"));out.print("\",\"");
				out.print(resultSetOracle.getString("表示名１"));out.print("\",\"");
				out.print(resultSetOracle.getString("型式上２桁２"));out.print("\",\"");
				out.print(resultSetOracle.getString("年改番号２"));out.print("\",\"");
				out.print(resultSetOracle.getString("表示名２"));out.print("\",\"");
				out.print(resultSetOracle.getInt("基本整備費"));out.print("\",\"");
				out.print(resultSetOracle.getInt("電子完検適用開始日"));out.print("\",\"");
				out.print(resultSetOracle.getInt("電子完検適用終了日"));out.print("\"");out.print(System.getProperty("line.separator"));
//				int updateCount = _statementMySQL.executeUpdate("INSERT INTO customer_d (customer_code,customer_name,shop_name,sales_name) VALUES ('"+customer_code+"','"+customer_name+"','"+shop_name+"','"+sales_name+"')");

//				System.out.println("お客様コード: " + customer_code + ", お客様名: " + customer_name + ", 担当拠点: " + shop_name + ",担当セールス: " + sales_name);
//				System.out.println("***** " + edit_date + " データSELECT *****");
				RecordCount++;
			}
			out.close();
			cal     = Calendar.getInstance();
			edit_date = (df.format(cal.getTime()));
			System.out.println("***** " + edit_date + " 新車車種DIM CSVファイル出力終了 出力件数=" + RecordCount + "件 *****");
		}finally{
			resultSetOracle.close();
		}
	}

	//DIM_陸運事務所マスタ
	private void executeSelect6()
		throws SQLException{
		cal     = Calendar.getInstance();
		edit_date = (df.format(cal.getTime()));
		System.out.println("***** " + edit_date + " 陸運事務所DIM CSVファイル出力開始 *****");
		String SQL;
		SQL = "SELECT PTYM0150.陸事コード, PTYM0150.作成年月日, PTYM0150.作成時間, PTYM0150.作成ＴＲＡＮ, PTYM0150.作成端末ＩＰ, PTYM0150.作成社員コード, PTYM0150.更新年月日, PTYM0150.更新時間, PTYM0150.更新ＴＲＡＮ, PTYM0150.更新端末ＩＰ, PTYM0150.更新社員コード, PTYM0150.標板文字, PTYM0150.\"標板文字（カナ）\", PTYM0150.支局名, PTYM0150.\"陸事コード（４桁）\" FROM PTYM0150";
		ResultSet resultSetOracle = _statementOracle.executeQuery(SQL);
		try{
//			boolean br = resultSet.first();
//			if(br == false) {
//				return;
//			}
			try {
				out = new PrintWriter(csvfile6);
			} catch (FileNotFoundException e) {
				// TODO 自動生成された catch ブロック
				e.printStackTrace();
			}
			//見出し出力
			out.print("\"陸事コード\",\"作成年月日\",\"作成時間\",\"作成ＴＲＡＮ\",\"作成端末ＩＰ\",\"作成社員コード\",\"更新年月日\",\"更新時間\",\"更新ＴＲＡＮ\",\"更新端末ＩＰ\",\"更新社員コード\",\"標板文字\",\"標板文字（カナ）\",\"支局名\",\"陸事コード（４桁）\"");out.print(System.getProperty("line.separator"));

			long RecordCount = 0;
			while(resultSetOracle.next()){

				out.print("\"");out.print(resultSetOracle.getString("陸事コード"));out.print("\",\"");
				out.print(resultSetOracle.getInt("作成年月日"));out.print("\",\"");
				out.print(resultSetOracle.getInt("作成時間"));out.print("\",\"");
				out.print(resultSetOracle.getString("作成ＴＲＡＮ"));out.print("\",\"");
				out.print(resultSetOracle.getString("作成端末ＩＰ"));out.print("\",\"");
				out.print(resultSetOracle.getString("作成社員コード"));out.print("\",\"");
				out.print(resultSetOracle.getInt("更新年月日"));out.print("\",\"");
				out.print(resultSetOracle.getInt("更新時間"));out.print("\",\"");
				out.print(resultSetOracle.getString("更新ＴＲＡＮ"));out.print("\",\"");
				out.print(resultSetOracle.getString("更新端末ＩＰ"));out.print("\",\"");
				out.print(resultSetOracle.getString("更新社員コード"));out.print("\",\"");
				out.print(resultSetOracle.getString("標板文字"));out.print("\",\"");
				out.print(resultSetOracle.getString("標板文字（カナ）"));out.print("\",\"");
				out.print(resultSetOracle.getString("支局名"));out.print("\",\"");
				out.print(resultSetOracle.getString("陸事コード（４桁）"));out.print("\"");out.print(System.getProperty("line.separator"));
//				int updateCount = _statementMySQL.executeUpdate("INSERT INTO customer_d (customer_code,customer_name,shop_name,sales_name) VALUES ('"+customer_code+"','"+customer_name+"','"+shop_name+"','"+sales_name+"')");

//				System.out.println("お客様コード: " + customer_code + ", お客様名: " + customer_name + ", 担当拠点: " + shop_name + ",担当セールス: " + sales_name);
//				System.out.println("***** " + edit_date + " データSELECT *****");
				RecordCount++;
			}
			out.close();
			cal     = Calendar.getInstance();
			edit_date = (df.format(cal.getTime()));
			System.out.println("***** " + edit_date + " 陸運事務所DIM CSVファイル出力終了 出力件数=" + RecordCount + "件 *****");
		}finally{
			resultSetOracle.close();
		}
	}

	//拠点診断カルテ明細FACT
	private void executeSelect7()
		throws SQLException{
		cal     = Calendar.getInstance();
		edit_date = (df.format(cal.getTime()));
		System.out.println("***** " + edit_date + " 拠点診断カルテ明細FACT CSVファイル出力開始 *****");
		String SQL;
		SQL = "SELECT PTAD1450.カルテ明細番号, PTAD1450.作成年月日, PTAD1450.作成時間, PTAD1450.作成ＴＲＡＮ, PTAD1450.作成端末ＩＰ, PTAD1450.作成社員コード, PTAD1450.更新年月日, PTAD1450.更新時間, PTAD1450.更新ＴＲＡＮ, PTAD1450.更新端末ＩＰ, PTAD1450.更新社員コード, PTAD1450.担当拠点コード, PTAD1450.担当セールスコード, PTAD1450.業販店コード, PTAD1450.お客様コード, PTAD1450.お客様名１, PTAD1450.お客様名２, PTAD1450.フリガナ１, PTAD1450.フリガナ２, PTAD1450.お客様名編集区分, PTAD1450.住所１, PTAD1450.住所２, PTAD1450.住所３, PTAD1450.電話番号, PTAD1450.\"電話番号（携）\", PTAD1450.\"Ｅ－ＭＡＩＬ\", PTAD1450.顧客区分, PTAD1450.車台番号, PTAD1450.初度登録年月, PTAD1450.登録番号, PTAD1450.車種名, PTAD1450.型式, PTAD1450.ＯＰ, PTAD1450.仕様, PTAD1450.車名コード, PTAD1450.自動車種別コード, PTAD1450.整備担当拠点コード, PTAD1450.活動依頼区分, PTAD1450.入庫拠点コード, PTAD1450.契機コード, PTAD1450.基準日, PTAD1450.初回車検区分, PTAD1450.サービス受注番号, PTAD1450.サービスご用命事項, PTAD1450.納車日, PTAD1450.メカニックコード, PTAD1450.推奨点検内容, PTAD1450.保険会社コード, PTAD1450.証券番号, PTAD1450.証券枝番号, PTAD1450.自賠責入力番号, PTAD1450.注文書番号, PTAD1450.追加区分, PTAD1450.活動年月日, PTAD1450.次回活動予定年月日, PTAD1450.活動コメント, PTAD1450.ホット度, PTAD1450.販売見込車種型式, PTAD1450.成約コード, PTAD1450.敗戦理由コード, PTAD1450.活動中止コード, PTAD1450.入庫促進コード, PTAD1450.商談内容, PTAD1450.フロント対応結果, PTAD1450.フロント敗戦理由, PTAD1450.フロント活動中止, PTAD1450.フロント対応年月日, PTAD1450.引取要否区分, PTAD1450.代車要否区分, PTAD1450.\"サ－お客様コメント\", PTAD1450.フロント接触相手, PTAD1450.報告対象年月１, PTAD1450.実施未実施区分１, PTAD1450.実施年月日１, PTAD1450.報告対象年月２, PTAD1450.実施未実施区分２, PTAD1450.実施年月日２, PTAD1450.報告対象年月３, PTAD1450.実施未実施区分３, PTAD1450.実施年月日３, PTAD1450.車検対応結果区分, PTAD1450.車両保険付保区分, PTAD1450.フォロー明細年月, PTAD1450.点検パックコード, PTAD1450.点検パック終了日 FROM PTAD1450";
		ResultSet resultSetOracle = _statementOracle.executeQuery(SQL);
		try{
//			boolean br = resultSet.first();
//			if(br == false) {
//				return;
//			}
			try {
				out = new PrintWriter(csvfile7);
			} catch (FileNotFoundException e) {
				// TODO 自動生成された catch ブロック
				e.printStackTrace();
			}
			//見出し出力
			out.print("\"カルテ明細番号\",\"作成年月日\",\"作成時間\",\"作成ＴＲＡＮ\",\"作成端末ＩＰ\",\"作成社員コード\",\"更新年月日\",\"更新時間\",\"更新ＴＲＡＮ\",\"更新端末ＩＰ\",\"更新社員コード\",\"担当拠点コード\",\"担当セールスコード\",\"業販店コード\",\"お客様コード\",\"お客様名１\",\"お客様名２\",\"フリガナ１\",\"フリガナ２\",\"お客様名編集区分\",\"住所１\",\"住所２\",\"住所３\",\"電話番号\",\"電話番号（携）\",\"Ｅ－ＭＡＩＬ\",\"顧客区分\",\"車台番号\",\"初度登録年月\",\"登録番号\",\"車種名\",\"型式\",\"ＯＰ\",\"仕様\",\"車名コード\",\"自動車種別コード\",\"整備担当拠点コード\",\"活動依頼区分\",\"入庫拠点コード\",\"契機コード\",\"基準日\",\"初回車検区分\",\"サービス受注番号\",\"サービスご用命事項\",\"納車日\",\"メカニックコード\",\"推奨点検内容\",\"保険会社コード\",\"証券番号\",\"証券枝番号\",\"自賠責入力番号\",\"注文書番号\",\"追加区分\",\"活動年月日\",\"次回活動予定年月日\",\"活動コメント\",\"ホット度\",\"販売見込車種型式\",\"成約コード\",\"敗戦理由コード\",\"活動中止コード\",\"入庫促進コード\",\"商談内容\",\"フロント対応結果\",\"フロント敗戦理由\",\"フロント活動中止\",\"フロント対応年月日\",\"引取要否区分\",\"代車要否区分\",\"サ－お客様コメント\",\"フロント接触相手\",\"報告対象年月１\",\"実施未実施区分１\",\"実施年月日１\",\"報告対象年月２\",\"実施未実施区分２\",\"実施年月日２\",\"報告対象年月３\",\"実施未実施区分３\",\"実施年月日３\",\"車検対応結果区分\",\"車両保険付保区分\",\"フォロー明細年月\",\"点検パックコード\",\"点検パック終了日\"");out.print(System.getProperty("line.separator"));

			long RecordCount = 0;
			while(resultSetOracle.next()){

				out.print("\"");out.print(resultSetOracle.getString("カルテ明細番号"));out.print("\",\"");
				out.print(resultSetOracle.getInt("作成年月日"));out.print("\",\"");
				out.print(resultSetOracle.getInt("作成時間"));out.print("\",\"");
				out.print(resultSetOracle.getString("作成ＴＲＡＮ"));out.print("\",\"");
				out.print(resultSetOracle.getString("作成端末ＩＰ"));out.print("\",\"");
				out.print(resultSetOracle.getString("作成社員コード"));out.print("\",\"");
				out.print(resultSetOracle.getInt("更新年月日"));out.print("\",\"");
				out.print(resultSetOracle.getInt("更新時間"));out.print("\",\"");
				out.print(resultSetOracle.getString("更新ＴＲＡＮ"));out.print("\",\"");
				out.print(resultSetOracle.getString("更新端末ＩＰ"));out.print("\",\"");
				out.print(resultSetOracle.getString("更新社員コード"));out.print("\",\"");
				out.print(resultSetOracle.getString("担当拠点コード"));out.print("\",\"");
				out.print(resultSetOracle.getString("担当セールスコード"));out.print("\",\"");
				out.print(resultSetOracle.getString("業販店コード"));out.print("\",\"");
				out.print(resultSetOracle.getString("お客様コード"));out.print("\",\"");
				out.print(resultSetOracle.getString("お客様名１"));out.print("\",\"");
				out.print(resultSetOracle.getString("お客様名２"));out.print("\",\"");
				out.print(resultSetOracle.getString("フリガナ１"));out.print("\",\"");
				out.print(resultSetOracle.getString("フリガナ２"));out.print("\",\"");
				out.print(resultSetOracle.getString("お客様名編集区分"));out.print("\",\"");
				out.print(resultSetOracle.getString("住所１"));out.print("\",\"");
				out.print(resultSetOracle.getString("住所２"));out.print("\",\"");
				out.print(resultSetOracle.getString("住所３"));out.print("\",\"");
				out.print(resultSetOracle.getString("電話番号"));out.print("\",\"");
				out.print(resultSetOracle.getString("電話番号（携）"));out.print("\",\"");
				out.print(resultSetOracle.getString("Ｅ－ＭＡＩＬ"));out.print("\",\"");
				out.print(resultSetOracle.getString("顧客区分"));out.print("\",\"");
				out.print(resultSetOracle.getString("車台番号"));out.print("\",\"");
				out.print(resultSetOracle.getInt("初度登録年月"));out.print("\",\"");
				out.print(resultSetOracle.getString("登録番号"));out.print("\",\"");
				out.print(resultSetOracle.getString("車種名"));out.print("\",\"");
				out.print(resultSetOracle.getString("型式"));out.print("\",\"");
				out.print(resultSetOracle.getString("ＯＰ"));out.print("\",\"");
				out.print(resultSetOracle.getString("仕様"));out.print("\",\"");
				out.print(resultSetOracle.getString("車名コード"));out.print("\",\"");
				out.print(resultSetOracle.getString("自動車種別コード"));out.print("\",\"");
				out.print(resultSetOracle.getString("整備担当拠点コード"));out.print("\",\"");
				out.print(resultSetOracle.getString("活動依頼区分"));out.print("\",\"");
				out.print(resultSetOracle.getString("入庫拠点コード"));out.print("\",\"");
				out.print(resultSetOracle.getString("契機コード"));out.print("\",\"");
				out.print(resultSetOracle.getInt("基準日"));out.print("\",\"");
				out.print(resultSetOracle.getString("初回車検区分"));out.print("\",\"");
				out.print(resultSetOracle.getString("サービス受注番号"));out.print("\",\"");
				out.print(resultSetOracle.getString("サービスご用命事項"));out.print("\",\"");
				out.print(resultSetOracle.getInt("納車日"));out.print("\",\"");
				out.print(resultSetOracle.getString("メカニックコード"));out.print("\",\"");
				out.print(resultSetOracle.getString("推奨点検内容"));out.print("\",\"");
				out.print(resultSetOracle.getString("保険会社コード"));out.print("\",\"");
				out.print(resultSetOracle.getString("証券番号"));out.print("\",\"");
				out.print(resultSetOracle.getString("証券枝番号"));out.print("\",\"");
				out.print(resultSetOracle.getString("自賠責入力番号"));out.print("\",\"");
				out.print(resultSetOracle.getString("注文書番号"));out.print("\",\"");
				out.print(resultSetOracle.getString("追加区分"));out.print("\",\"");
				out.print(resultSetOracle.getInt("活動年月日"));out.print("\",\"");
				out.print(resultSetOracle.getInt("次回活動予定年月日"));out.print("\",\"");
				out.print(resultSetOracle.getString("活動コメント"));out.print("\",\"");
				out.print(resultSetOracle.getString("ホット度"));out.print("\",\"");
				out.print(resultSetOracle.getString("販売見込車種型式"));out.print("\",\"");
				out.print(resultSetOracle.getString("成約コード"));out.print("\",\"");
				out.print(resultSetOracle.getString("敗戦理由コード"));out.print("\",\"");
				out.print(resultSetOracle.getString("活動中止コード"));out.print("\",\"");
				out.print(resultSetOracle.getString("入庫促進コード"));out.print("\",\"");
				out.print(resultSetOracle.getString("商談内容"));out.print("\",\"");
				out.print(resultSetOracle.getString("フロント対応結果"));out.print("\",\"");
				out.print(resultSetOracle.getString("フロント敗戦理由"));out.print("\",\"");
				out.print(resultSetOracle.getString("フロント活動中止"));out.print("\",\"");
				out.print(resultSetOracle.getInt("フロント対応年月日"));out.print("\",\"");
				out.print(resultSetOracle.getString("引取要否区分"));out.print("\",\"");
				out.print(resultSetOracle.getString("代車要否区分"));out.print("\",\"");
				out.print(resultSetOracle.getString("サ－お客様コメント"));out.print("\",\"");
				out.print(resultSetOracle.getString("フロント接触相手"));out.print("\",\"");
				out.print(resultSetOracle.getInt("報告対象年月１"));out.print("\",\"");
				out.print(resultSetOracle.getString("実施未実施区分１"));out.print("\",\"");
				out.print(resultSetOracle.getInt("実施年月日１"));out.print("\",\"");
				out.print(resultSetOracle.getInt("報告対象年月２"));out.print("\",\"");
				out.print(resultSetOracle.getString("実施未実施区分２"));out.print("\",\"");
				out.print(resultSetOracle.getInt("実施年月日２"));out.print("\",\"");
				out.print(resultSetOracle.getInt("報告対象年月３"));out.print("\",\"");
				out.print(resultSetOracle.getString("実施未実施区分３"));out.print("\",\"");
				out.print(resultSetOracle.getInt("実施年月日３"));out.print("\",\"");
				out.print(resultSetOracle.getString("車検対応結果区分"));out.print("\",\"");
				out.print(resultSetOracle.getString("車両保険付保区分"));out.print("\",\"");
				out.print(resultSetOracle.getInt("フォロー明細年月"));out.print("\",\"");
				out.print(resultSetOracle.getString("点検パックコード"));out.print("\",\"");
				out.print(resultSetOracle.getInt("点検パック終了日"));out.print("\"");out.print(System.getProperty("line.separator"));
//				int updateCount = _statementMySQL.executeUpdate("INSERT INTO customer_d (customer_code,customer_name,shop_name,sales_name) VALUES ('"+customer_code+"','"+customer_name+"','"+shop_name+"','"+sales_name+"')");

//				System.out.println("お客様コード: " + customer_code + ", お客様名: " + customer_name + ", 担当拠点: " + shop_name + ",担当セールス: " + sales_name);
//				System.out.println("***** " + edit_date + " データSELECT *****");
				RecordCount++;
			}
			out.close();
			cal     = Calendar.getInstance();
			edit_date = (df.format(cal.getTime()));
			System.out.println("***** " + edit_date + " 拠点診断カルテ明細FACT CSVファイル出力終了 出力件数=" + RecordCount + "件 *****");
		}finally{
			resultSetOracle.close();
		}
	}

	//社員DIM
	private void executeSelect8()
		throws SQLException{
		cal     = Calendar.getInstance();
		edit_date = (df.format(cal.getTime()));
		System.out.println("***** " + edit_date + " 社員DIM CSVファイル出力開始 *****");
		String SQL;
		SQL = "SELECT PTYM0120.社員コード, PTYM0120.社員名, PTYM0120.所属拠点コード, PTYM0130.拠点名 FROM PTYM0120 INNER JOIN PTYM0130 ON PTYM0120.所属拠点コード = PTYM0130.拠点コード";
		ResultSet resultSetOracle = _statementOracle.executeQuery(SQL);
		try{
//			boolean br = resultSet.first();
//			if(br == false) {
//				return;
//			}
			try {
				out = new PrintWriter(csvfile8);
			} catch (FileNotFoundException e) {
				// TODO 自動生成された catch ブロック
				e.printStackTrace();
			}
			//見出し出力
			out.print("\"社員コード\",\"社員名\",\"所属拠点コード\",\"拠点名\"");out.print(System.getProperty("line.separator"));

			long RecordCount = 0;
			while(resultSetOracle.next()){

				out.print("\"");out.print(resultSetOracle.getString("社員コード"));out.print("\",\"");
				out.print(resultSetOracle.getString("社員名"));out.print("\",\"");
				out.print(resultSetOracle.getString("所属拠点コード"));out.print("\",\"");
				out.print(resultSetOracle.getString("拠点名"));out.print("\"");out.print(System.getProperty("line.separator"));
//				int updateCount = _statementMySQL.executeUpdate("INSERT INTO customer_d (customer_code,customer_name,shop_name,sales_name) VALUES ('"+customer_code+"','"+customer_name+"','"+shop_name+"','"+sales_name+"')");

//				System.out.println("お客様コード: " + customer_code + ", お客様名: " + customer_name + ", 担当拠点: " + shop_name + ",担当セールス: " + sales_name);
//				System.out.println("***** " + edit_date + " データSELECT *****");
				RecordCount++;
			}
			out.close();
			cal     = Calendar.getInstance();
			edit_date = (df.format(cal.getTime()));
			System.out.println("***** " + edit_date + " 社員DIM CSVファイル出力終了 出力件数=" + RecordCount + "件 *****");
		}finally{
			resultSetOracle.close();
		}
	}

	//新車販売目標FACT
	private void executeSelect9()
		throws SQLException{
		cal     = Calendar.getInstance();
		edit_date = (df.format(cal.getTime()));
		System.out.println("***** " + edit_date + " 新車販売目標FACT CSVファイル出力開始 *****");
		String SQL;
		SQL = "SELECT PTRDA310.年月, PTRDA310.拠点コード, PTRDA310.車種グループ, PTRDA310.作成年月日, PTRDA310.作成時間, PTRDA310.作成ＴＲＡＮ, PTRDA310.作成端末ＩＰ, PTRDA310.作成社員コード, PTRDA310.更新年月日, PTRDA310.更新時間, PTRDA310.更新ＴＲＡＮ, PTRDA310.更新端末ＩＰ, PTRDA310.更新社員コード, PTRDA310.\"協約売上台数目標（直販）\", PTRDA310.\"協約売上台数目標（業販）\", PTRDA310.\"協約売上台数目標（農販）\", PTRDA310.\"計画売上台数目標（直販）\", PTRDA310.\"計画売上台数目標（業販）\", PTRDA310.\"計画売上台数目標（農販）\", PTRDA310.\"売上台数実績（直販）\", PTRDA310.\"売上台数実績（業販）\", PTRDA310.\"売上台数実績（農販）\", PTRDA310.\"前月受注残台数（直販）\", PTRDA310.\"前月受注残台数（業販）\", PTRDA310.\"前月受注残台数（農販）\", PTRDA310.\"当月受注台数（直販）\", PTRDA310.\"当月受注台数（業販）\", PTRDA310.\"当月受注台数（農販）\", PTRDA310.\"当月予定登録台数（直販）\", PTRDA310.\"当月外予定登録台数（直販）\", PTRDA310.\"当月予定登録台数（業販）\", PTRDA310.\"当月外予定登録台数（業販）\", PTRDA310.\"当月予定登録台数（農販）\", PTRDA310.\"当月外予定登録台数（農販）\", PTRDA310.\"納車台数（直販）\", PTRDA310.\"納車台数（業販）\", PTRDA310.\"納車台数（農販）\" FROM PTRDA310";
		ResultSet resultSetOracle = _statementOracle.executeQuery(SQL);
		try{
//			boolean br = resultSet.first();
//			if(br == false) {
//				return;
//			}
			try {
				out = new PrintWriter(csvfile9);
			} catch (FileNotFoundException e) {
				// TODO 自動生成された catch ブロック
				e.printStackTrace();
			}
			//見出し出力
			out.print("\"年月\",\"拠点コード\",\"車種グループ\",\"作成年月日\",\"作成時間\",\"作成ＴＲＡＮ\",\"作成端末ＩＰ\",\"作成社員コード\",\"更新年月日\",\"更新時間\",\"更新ＴＲＡＮ\",\"更新端末ＩＰ\",\"更新社員コード\",\"協約売上台数目標（直販）\",\"協約売上台数目標（業販）\",\"協約売上台数目標（農販）\",\"計画売上台数目標（直販）\",\"計画売上台数目標（業販）\",\"計画売上台数目標（農販）\",\"売上台数実績（直販）\",\"売上台数実績（業販）\",\"売上台数実績（農販）\",\"前月受注残台数（直販）\",\"前月受注残台数（業販）\",\"前月受注残台数（農販）\",\"当月受注台数（直販）\",\"当月受注台数（業販）\",\"当月受注台数（農販）\",\"当月予定登録台数（直販）\",\"当月外予定登録台数（直販）\",\"当月予定登録台数（業販）\",\"当月外予定登録台数（業販）\",\"当月予定登録台数（農販）\",\"当月外予定登録台数（農販）\",\"納車台数（直販）\",\"納車台数（業販）\",\"納車台数（農販）\"");out.print(System.getProperty("line.separator"));

			long RecordCount = 0;
			while(resultSetOracle.next()){

				out.print("\"");out.print(resultSetOracle.getString("年月"));out.print("\",\"");
				out.print(resultSetOracle.getString("拠点コード"));out.print("\",\"");
				out.print(resultSetOracle.getString("車種グループ"));out.print("\",\"");
				out.print(resultSetOracle.getInt("作成年月日"));out.print("\",\"");
				out.print(resultSetOracle.getInt("作成時間"));out.print("\",\"");
				out.print(resultSetOracle.getString("作成ＴＲＡＮ"));out.print("\",\"");
				out.print(resultSetOracle.getString("作成端末ＩＰ"));out.print("\",\"");
				out.print(resultSetOracle.getString("作成社員コード"));out.print("\",\"");
				out.print(resultSetOracle.getInt("更新年月日"));out.print("\",\"");
				out.print(resultSetOracle.getInt("更新時間"));out.print("\",\"");
				out.print(resultSetOracle.getString("更新ＴＲＡＮ"));out.print("\",\"");
				out.print(resultSetOracle.getString("更新端末ＩＰ"));out.print("\",\"");
				out.print(resultSetOracle.getString("更新社員コード"));out.print("\",\"");
				out.print(resultSetOracle.getInt("協約売上台数目標（直販）"));out.print("\",\"");
				out.print(resultSetOracle.getInt("協約売上台数目標（業販）"));out.print("\",\"");
				out.print(resultSetOracle.getInt("協約売上台数目標（農販）"));out.print("\",\"");
				out.print(resultSetOracle.getInt("計画売上台数目標（直販）"));out.print("\",\"");
				out.print(resultSetOracle.getInt("計画売上台数目標（業販）"));out.print("\",\"");
				out.print(resultSetOracle.getInt("計画売上台数目標（農販）"));out.print("\",\"");
				out.print(resultSetOracle.getInt("売上台数実績（直販）"));out.print("\",\"");
				out.print(resultSetOracle.getInt("売上台数実績（業販）"));out.print("\",\"");
				out.print(resultSetOracle.getInt("売上台数実績（農販）"));out.print("\",\"");
				out.print(resultSetOracle.getInt("前月受注残台数（直販）"));out.print("\",\"");
				out.print(resultSetOracle.getInt("前月受注残台数（業販）"));out.print("\",\"");
				out.print(resultSetOracle.getInt("前月受注残台数（農販）"));out.print("\",\"");
				out.print(resultSetOracle.getInt("当月受注台数（直販）"));out.print("\",\"");
				out.print(resultSetOracle.getInt("当月受注台数（業販）"));out.print("\",\"");
				out.print(resultSetOracle.getInt("当月受注台数（農販）"));out.print("\",\"");
				out.print(resultSetOracle.getInt("当月予定登録台数（直販）"));out.print("\",\"");
				out.print(resultSetOracle.getInt("当月外予定登録台数（直販）"));out.print("\",\"");
				out.print(resultSetOracle.getInt("当月予定登録台数（業販）"));out.print("\",\"");
				out.print(resultSetOracle.getInt("当月外予定登録台数（業販）"));out.print("\",\"");
				out.print(resultSetOracle.getInt("当月予定登録台数（農販）"));out.print("\",\"");
				out.print(resultSetOracle.getInt("当月外予定登録台数（農販）"));out.print("\",\"");
				out.print(resultSetOracle.getInt("納車台数（直販）"));out.print("\",\"");
				out.print(resultSetOracle.getInt("納車台数（業販）"));out.print("\",\"");
				out.print(resultSetOracle.getInt("納車台数（農販）"));out.print("\"");out.print(System.getProperty("line.separator"));
//				int updateCount = _statementMySQL.executeUpdate("INSERT INTO customer_d (customer_code,customer_name,shop_name,sales_name) VALUES ('"+customer_code+"','"+customer_name+"','"+shop_name+"','"+sales_name+"')");

//				System.out.println("お客様コード: " + customer_code + ", お客様名: " + customer_name + ", 担当拠点: " + shop_name + ",担当セールス: " + sales_name);
//				System.out.println("***** " + edit_date + " データSELECT *****");
				RecordCount++;
			}
			out.close();
			cal     = Calendar.getInstance();
			edit_date = (df.format(cal.getTime()));
			System.out.println("***** " + edit_date + " 新車販売目標FACT CSVファイル出力終了 出力件数=" + RecordCount + "件 *****");
		}finally{
			resultSetOracle.close();
		}
	}

	//新車受注明細FACT
	private void executeSelect10()
		throws SQLException{
		cal     = Calendar.getInstance();
		edit_date = (df.format(cal.getTime()));
		System.out.println("***** " + edit_date + " 新車受注明細FACT CSVファイル出力開始 *****");
		String SQL;
		SQL = "SELECT PTBD9310.受注管理番号, PTBD9310.注文書番号, PTBD9310.受注拠点コード, PTBD9310.商談担当者コード, PTBD9310.車種コード, PTBD9310.\"型式（全銘柄）\", PTBD9310.ＯＰ, PTBD9310.仕様, PTBD9310.注文者お客様ＣＤ, PTBD9310.注文者氏名１, PTBD9310.注文者氏名２, PTBD9310.注文者生年月日, PTBD9310.査定書番号, PTBD9320.販売経路, PTBD9320.支払い総額, PTBD9320.車両本体価格, PTBD9320.車両本体価格値引額, PTBD9320.付属品価格計, PTBD9320.付属品価格値引額, PTBD9320.支払方法, PTBD9320.現金, PTBD9320.割賦元金等, PTBD9320.道路サービス選択, PTBD9320.他預法定名称２, PTBD9320.他預法定１フラグ, PTBD9320.紹介者区分, PTBD9320.紹介者コード, PTBD9320.紹介者氏名１, PTBD9320.紹介者氏名２, PTBD9320.紹介料, PTBD9320.仲介店コード, PTBD9320.仲介店名１, PTBD9320.仲介店名２, PTBD9320.仲介店取扱料支払額, PTBD9340.在庫管理番号, PTBD9340.契約キャンセル日, PTBD9340.拠点長承認実績日, PTBD9340.引当実績日, PTBD9340.登録実績日, PTBD9340.納車実績日, PTBD9340.注文書コメント１, PTBD9340.注文書コメント２, PTBD9350.使用者氏名１, PTBD9350.使用者氏名２, PTBD9350.使用者お客様ＣＤ ";
		SQL = SQL + " FROM ((PTBD9310 INNER JOIN PTBD9320 ON PTBD9310.受注管理番号 = PTBD9320.受注管理番号) INNER JOIN PTBD9340 ON PTBD9310.受注管理番号 = PTBD9340.受注管理番号) INNER JOIN PTBD9350 ON PTBD9310.受注管理番号 = PTBD9350.受注管理番号";
		ResultSet resultSetOracle = _statementOracle.executeQuery(SQL);
		try{
//			boolean br = resultSet.first();
//			if(br == false) {
//				return;
//			}
			try {
				out = new PrintWriter(csvfile10);
			} catch (FileNotFoundException e) {
				// TODO 自動生成された catch ブロック
				e.printStackTrace();
			}
			//見出し出力
			out.print("\"受注管理番号\",\"注文書番号\",\"受注拠点コード\",\"商談担当者コード\",\"車種コード\",\"型式（全銘柄）\",\"ＯＰ\",\"仕様\",\"注文者お客様ＣＤ\",\"注文者氏名１\",\"注文者氏名２\",\"注文者生年月日\",\"査定書番号\",\"販売経路\",\"支払い総額\",\"車両本体価格\",\"車両本体価格値引額\",\"付属品価格計\",\"付属品価格値引額\",\"支払方法\",\"現金\",\"割賦元金等\",\"道路サービス選択\",\"他預法定名称２\",\"他預法定１フラグ\",\"紹介者区分\",\"紹介者コード\",\"紹介者氏名１\",\"紹介者氏名２\",\"紹介料\",\"仲介店コード\",\"仲介店名１\",\"仲介店名２\",\"仲介店取扱料支払額\",\"在庫管理番号\",\"契約キャンセル日\",\"拠点長承認実績日\",\"引当実績日\",\"登録実績日\",\"納車実績日\",\"注文書コメント１\",\"注文書コメント２\",\"使用者氏名１\",\"使用者氏名２\",\"使用者お客様ＣＤ\"");out.print(System.getProperty("line.separator"));

			long RecordCount = 0;
			while(resultSetOracle.next()){

				out.print("\"");out.print(resultSetOracle.getString("受注管理番号"));out.print("\",\"");
				out.print(resultSetOracle.getString("注文書番号"));out.print("\",\"");
				out.print(resultSetOracle.getString("受注拠点コード"));out.print("\",\"");
				out.print(resultSetOracle.getString("商談担当者コード"));out.print("\",\"");
				out.print(resultSetOracle.getString("車種コード"));out.print("\",\"");
				out.print(resultSetOracle.getString("型式（全銘柄）"));out.print("\",\"");
				out.print(resultSetOracle.getString("ＯＰ"));out.print("\",\"");
				out.print(resultSetOracle.getString("仕様"));out.print("\",\"");
				out.print(resultSetOracle.getString("注文者お客様ＣＤ"));out.print("\",\"");
				out.print(resultSetOracle.getString("注文者氏名１"));out.print("\",\"");
				out.print(resultSetOracle.getString("注文者氏名２"));out.print("\",\"");
				out.print(resultSetOracle.getString("注文者生年月日"));out.print("\",\"");
				out.print(resultSetOracle.getString("査定書番号"));out.print("\",\"");
				out.print(resultSetOracle.getString("販売経路"));out.print("\",\"");
				out.print(resultSetOracle.getInt("支払い総額"));out.print("\",\"");
				out.print(resultSetOracle.getInt("車両本体価格"));out.print("\",\"");
				out.print(resultSetOracle.getInt("車両本体価格値引額"));out.print("\",\"");
				out.print(resultSetOracle.getInt("付属品価格計"));out.print("\",\"");
				out.print(resultSetOracle.getInt("付属品価格値引額"));out.print("\",\"");
				out.print(resultSetOracle.getString("支払方法"));out.print("\",\"");
				out.print(resultSetOracle.getInt("現金"));out.print("\",\"");
				out.print(resultSetOracle.getInt("割賦元金等"));out.print("\",\"");
				out.print(resultSetOracle.getString("道路サービス選択"));out.print("\",\"");
				out.print(resultSetOracle.getString("他預法定名称２"));out.print("\",\"");
				out.print(resultSetOracle.getString("他預法定１フラグ"));out.print("\",\"");
				out.print(resultSetOracle.getString("紹介者区分"));out.print("\",\"");
				out.print(resultSetOracle.getString("紹介者コード"));out.print("\",\"");
				out.print(resultSetOracle.getString("紹介者氏名１"));out.print("\",\"");
				out.print(resultSetOracle.getString("紹介者氏名２"));out.print("\",\"");
				out.print(resultSetOracle.getString("紹介料"));out.print("\",\"");
				out.print(resultSetOracle.getString("仲介店コード"));out.print("\",\"");
				out.print(resultSetOracle.getString("仲介店名１"));out.print("\",\"");
				out.print(resultSetOracle.getString("仲介店名２"));out.print("\",\"");
				out.print(resultSetOracle.getInt("仲介店取扱料支払額"));out.print("\",\"");
				out.print(resultSetOracle.getString("在庫管理番号"));out.print("\",\"");
				out.print(resultSetOracle.getInt("契約キャンセル日"));out.print("\",\"");
				out.print(resultSetOracle.getInt("拠点長承認実績日"));out.print("\",\"");
				out.print(resultSetOracle.getInt("引当実績日"));out.print("\",\"");
				out.print(resultSetOracle.getInt("登録実績日"));out.print("\",\"");
				out.print(resultSetOracle.getInt("納車実績日"));out.print("\",\"");
				out.print(resultSetOracle.getString("注文書コメント１"));out.print("\",\"");
				out.print(resultSetOracle.getString("注文書コメント２"));out.print("\",\"");
				out.print(resultSetOracle.getString("使用者氏名１"));out.print("\",\"");
				out.print(resultSetOracle.getString("使用者氏名２"));out.print("\",\"");
				out.print(resultSetOracle.getString("使用者お客様ＣＤ"));out.print("\"");out.print(System.getProperty("line.separator"));
//				int updateCount = _statementMySQL.executeUpdate("INSERT INTO customer_d (customer_code,customer_name,shop_name,sales_name) VALUES ('"+customer_code+"','"+customer_name+"','"+shop_name+"','"+sales_name+"')");

//				System.out.println("お客様コード: " + customer_code + ", お客様名: " + customer_name + ", 担当拠点: " + shop_name + ",担当セールス: " + sales_name);
//				System.out.println("***** " + edit_date + " データSELECT *****");
				RecordCount++;
			}
			out.close();
			cal     = Calendar.getInstance();
			edit_date = (df.format(cal.getTime()));
			System.out.println("***** " + edit_date + " 新車受注明細FACT CSVファイル出力終了 出力件数=" + RecordCount + "件 *****");
		}finally{
			resultSetOracle.close();
		}
	}

	//新車車種集計DIM
	private void executeSelect11()
		throws SQLException{
		cal     = Calendar.getInstance();
		edit_date = (df.format(cal.getTime()));
		System.out.println("***** " + edit_date + " 新車車種集計DIM CSVファイル出力開始 *****");
		String SQL;
		SQL = "SELECT PTRMA020.車種グループ, PTRMA020.作成年月日, PTRMA020.作成時間, PTRMA020.作成ＴＲＡＮ, PTRMA020.作成端末ＩＰ, PTRMA020.作成社員コード, PTRMA020.更新年月日, PTRMA020.更新時間, PTRMA020.更新ＴＲＡＮ, PTRMA020.更新端末ＩＰ, PTRMA020.更新社員コード, PTRMA020.車種グループ名称, PTRMA020.車種グループ略称 FROM PTRMA020";
		ResultSet resultSetOracle = _statementOracle.executeQuery(SQL);
		try{
//			boolean br = resultSet.first();
//			if(br == false) {
//				return;
//			}
			try {
				out = new PrintWriter(csvfile11);
			} catch (FileNotFoundException e) {
				// TODO 自動生成された catch ブロック
				e.printStackTrace();
			}
			//見出し出力
			out.print("\"車種グループ\",\"作成年月日\",\"作成時間\",\"作成ＴＲＡＮ\",\"作成端末ＩＰ\",\"作成社員コード\",\"更新年月日\",\"更新時間\",\"更新ＴＲＡＮ\",\"更新端末ＩＰ\",\"更新社員コード\",\"車種グループ名称\",\"車種グループ略称\"");out.print(System.getProperty("line.separator"));

			long RecordCount = 0;
			while(resultSetOracle.next()){

				out.print("\"");out.print(resultSetOracle.getString("車種グループ"));out.print("\",\"");
				out.print(resultSetOracle.getInt("作成年月日"));out.print("\",\"");
				out.print(resultSetOracle.getInt("作成時間"));out.print("\",\"");
				out.print(resultSetOracle.getString("作成ＴＲＡＮ"));out.print("\",\"");
				out.print(resultSetOracle.getString("作成端末ＩＰ"));out.print("\",\"");
				out.print(resultSetOracle.getString("作成社員コード"));out.print("\",\"");
				out.print(resultSetOracle.getInt("更新年月日"));out.print("\",\"");
				out.print(resultSetOracle.getInt("更新時間"));out.print("\",\"");
				out.print(resultSetOracle.getString("更新ＴＲＡＮ"));out.print("\",\"");
				out.print(resultSetOracle.getString("更新端末ＩＰ"));out.print("\",\"");
				out.print(resultSetOracle.getString("更新社員コード"));out.print("\",\"");
				out.print(resultSetOracle.getString("車種グループ名称"));out.print("\",\"");
				out.print(resultSetOracle.getString("車種グループ略称"));out.print("\"");out.print(System.getProperty("line.separator"));
//				int updateCount = _statementMySQL.executeUpdate("INSERT INTO customer_d (customer_code,customer_name,shop_name,sales_name) VALUES ('"+customer_code+"','"+customer_name+"','"+shop_name+"','"+sales_name+"')");

//				System.out.println("お客様コード: " + customer_code + ", お客様名: " + customer_name + ", 担当拠点: " + shop_name + ",担当セールス: " + sales_name);
//				System.out.println("***** " + edit_date + " データSELECT *****");
				RecordCount++;
			}
			out.close();
			cal     = Calendar.getInstance();
			edit_date = (df.format(cal.getTime()));
			System.out.println("***** " + edit_date + " 新車車種集計DIM CSVファイル出力終了 出力件数=" + RecordCount + "件 *****");
		}finally{
			resultSetOracle.close();
		}
	}

	//拠点DIM
	private void executeSelect12()
		throws SQLException{
		cal     = Calendar.getInstance();
		edit_date = (df.format(cal.getTime()));
		System.out.println("***** " + edit_date + " 拠点DIM CSVファイル出力開始 *****");
		String SQL;
		SQL = "SELECT PTYM0130.拠点コード, PTYM0130.作成年月日, PTYM0130.作成時間, PTYM0130.作成ＴＲＡＮ, PTYM0130.作成端末ＩＰ, PTYM0130.作成社員コード, PTYM0130.更新年月日, PTYM0130.更新時間, PTYM0130.更新ＴＲＡＮ, PTYM0130.更新端末ＩＰ, PTYM0130.更新社員コード, PTYM0130.拠点名, PTYM0130.拠点名略１, PTYM0130.拠点名略２, PTYM0130.郵便番号, PTYM0130.住所１, PTYM0130.住所２, PTYM0130.住所３, PTYM0130.電話番号, PTYM0130.ＦＡＸ番号, PTYM0130.\"拠点Ｅ－ＭＡＩＬ\", PTYM0130.本社拠点区分, PTYM0130.銀行コード, PTYM0130.支店コード, PTYM0130.口座種別区分, PTYM0130.口座番号, PTYM0130.口座名義人, PTYM0130.\"口座名義人（半角）\", PTYM0130.整備担当拠点コード, PTYM0130.\"部門コード（新）\", PTYM0130.\"部門コード（中）\", PTYM0130.\"部門コード（サ）\", PTYM0130.\"部門コード（部）\", PTYM0130.部門コード５, PTYM0130.部門コード６, PTYM0130.部門コード７, PTYM0130.部門コード８, PTYM0130.部門コード９, PTYM0130.部門コード１０, PTYM0130.ＦＨＩ拠点コード, PTYM0130.削除区分 FROM PTYM0130";
		ResultSet resultSetOracle = _statementOracle.executeQuery(SQL);
		try{
//			boolean br = resultSet.first();
//			if(br == false) {
//				return;
//			}
			try {
				out = new PrintWriter(csvfile12);
			} catch (FileNotFoundException e) {
				// TODO 自動生成された catch ブロック
				e.printStackTrace();
			}
			//見出し出力
			out.print("\"拠点コード\",\"作成年月日\",\"作成時間\",\"作成ＴＲＡＮ\",\"作成端末ＩＰ\",\"作成社員コード\",\"更新年月日\",\"更新時間\",\"更新ＴＲＡＮ\",\"更新端末ＩＰ\",\"更新社員コード\",\"拠点名\",\"拠点名略１\",\"拠点名略２\",\"郵便番号\",\"住所１\",\"住所２\",\"住所３\",\"電話番号\",\"ＦＡＸ番号\",\"拠点Ｅ－ＭＡＩＬ\",\"本社拠点区分\",\"銀行コード\",\"支店コード\",\"口座種別区分\",\"口座番号\",\"口座名義人\",\"口座名義人（半角）\",\"整備担当拠点コード\",\"部門コード（新）\",\"部門コード（中）\",\"部門コード（サ）\",\"部門コード（部）\",\"部門コード５\",\"部門コード６\",\"部門コード７\",\"部門コード８\",\"部門コード９\",\"部門コード１０\",\"ＦＨＩ拠点コード\",\"削除区分\"");out.print(System.getProperty("line.separator"));

			long RecordCount = 0;
			while(resultSetOracle.next()){

				out.print("\"");out.print(resultSetOracle.getString("拠点コード"));out.print("\",\"");
				out.print(resultSetOracle.getInt("作成年月日"));out.print("\",\"");
				out.print(resultSetOracle.getInt("作成時間"));out.print("\",\"");
				out.print(resultSetOracle.getString("作成ＴＲＡＮ"));out.print("\",\"");
				out.print(resultSetOracle.getString("作成端末ＩＰ"));out.print("\",\"");
				out.print(resultSetOracle.getString("作成社員コード"));out.print("\",\"");
				out.print(resultSetOracle.getInt("更新年月日"));out.print("\",\"");
				out.print(resultSetOracle.getInt("更新時間"));out.print("\",\"");
				out.print(resultSetOracle.getString("更新ＴＲＡＮ"));out.print("\",\"");
				out.print(resultSetOracle.getString("更新端末ＩＰ"));out.print("\",\"");
				out.print(resultSetOracle.getString("更新社員コード"));out.print("\",\"");
				out.print(resultSetOracle.getString("拠点名"));out.print("\",\"");
				out.print(resultSetOracle.getString("拠点名略１"));out.print("\",\"");
				out.print(resultSetOracle.getString("拠点名略２"));out.print("\",\"");
				out.print(resultSetOracle.getString("郵便番号"));out.print("\",\"");
				out.print(resultSetOracle.getString("住所１"));out.print("\",\"");
				out.print(resultSetOracle.getString("住所２"));out.print("\",\"");
				out.print(resultSetOracle.getString("住所３"));out.print("\",\"");
				out.print(resultSetOracle.getString("電話番号"));out.print("\",\"");
				out.print(resultSetOracle.getString("ＦＡＸ番号"));out.print("\",\"");
				out.print(resultSetOracle.getString("拠点Ｅ－ＭＡＩＬ"));out.print("\",\"");
				out.print(resultSetOracle.getString("本社拠点区分"));out.print("\",\"");
				out.print(resultSetOracle.getString("銀行コード"));out.print("\",\"");
				out.print(resultSetOracle.getString("支店コード"));out.print("\",\"");
				out.print(resultSetOracle.getString("口座種別区分"));out.print("\",\"");
				out.print(resultSetOracle.getString("口座番号"));out.print("\",\"");
				out.print(resultSetOracle.getString("口座名義人"));out.print("\",\"");
				out.print(resultSetOracle.getString("口座名義人（半角）"));out.print("\",\"");
				out.print(resultSetOracle.getString("整備担当拠点コード"));out.print("\",\"");
				out.print(resultSetOracle.getString("部門コード（新）"));out.print("\",\"");
				out.print(resultSetOracle.getString("部門コード（中）"));out.print("\",\"");
				out.print(resultSetOracle.getString("部門コード（サ）"));out.print("\",\"");
				out.print(resultSetOracle.getString("部門コード（部）"));out.print("\",\"");
				out.print(resultSetOracle.getString("部門コード５"));out.print("\",\"");
				out.print(resultSetOracle.getString("部門コード６"));out.print("\",\"");
				out.print(resultSetOracle.getString("部門コード７"));out.print("\",\"");
				out.print(resultSetOracle.getString("部門コード８"));out.print("\",\"");
				out.print(resultSetOracle.getString("部門コード９"));out.print("\",\"");
				out.print(resultSetOracle.getString("部門コード１０"));out.print("\",\"");
				out.print(resultSetOracle.getString("ＦＨＩ拠点コード"));out.print("\",\"");
				out.print(resultSetOracle.getString("削除区分"));out.print("\"");out.print(System.getProperty("line.separator"));
//				int updateCount = _statementMySQL.executeUpdate("INSERT INTO customer_d (customer_code,customer_name,shop_name,sales_name) VALUES ('"+customer_code+"','"+customer_name+"','"+shop_name+"','"+sales_name+"')");

//				System.out.println("お客様コード: " + customer_code + ", お客様名: " + customer_name + ", 担当拠点: " + shop_name + ",担当セールス: " + sales_name);
//				System.out.println("***** " + edit_date + " データSELECT *****");
				RecordCount++;
			}
			out.close();
			cal     = Calendar.getInstance();
			edit_date = (df.format(cal.getTime()));
			System.out.println("***** " + edit_date + " 拠点DIM CSVファイル出力終了 出力件数=" + RecordCount + "件 *****");
		}finally{
			resultSetOracle.close();
		}
	}

	/**
	 * TRUNCATE処理を実行します。
	 * @param
	 */
//	private void executeTRUNCATE()
//		throws SQLException{
		// SQL文を発行
//		_statementMySQL.executeUpdate("TRUNCATE TABLE customer_d");
//		cal     = Calendar.getInstance();
//		edit_date = (df.format(cal.getTime()));
//		System.out.println("***** " + edit_date + " 顧客データ削除終了 *****");
//	}

}
