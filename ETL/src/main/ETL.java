package main;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * ユーザデータベースへのアクセステストクラス。
 */
public class ETL {

	/**
	 * テーブル名。
	 */
	private static final String TABLE_NAME = "kyoten_m";

	/**
	 * テスト処理を実行します。
	 * @param args
	 */
	public static void main(String[] args) {
		ETL ETL = new ETL();

		try{
			// オブジェクトを生成
			ETL.create();

			// データ操作
			ETL.execute(args);
		}catch(Throwable t) {
			t.printStackTrace();
		}finally{
			// オブジェクトを破棄
			ETL.close();
		}
	}

	/**
	 * Connectionオブジェクトを保持します。
	 */
	private Connection _connection;

	/**
	 * Statementオブジェクトを保持します。
	 */
	private Statement _statement;

	/**
	 * 構築します。
	 */
	public ETL() {
		_connection = null;
		_statement = null;
	}

	/**
	 * オブジェクトを生成します。
	 */
	public void create()
		throws ClassNotFoundException, SQLException{
		// 下準備
		Class.forName("com.mysql.jdbc.Driver");
		_connection = DriverManager.getConnection("jdbc:mysql://10.3.129.1:3306/JAF?useUnicode=true&amp;characterEncoding=UTF-8", "partner21", "password");
		_statement = _connection.createStatement();
	}

	/**
	 * 各種オブジェクトを閉じます。
	 */
	public void close() {
		if(_statement != null) {
			try{
				_statement.close();
			}catch(SQLException e) {
				;
			}
			_statement = null;
		}
		if(_connection != null) {
			try{
				_connection.close();
			}catch(SQLException e) {
				;
			}
			_connection = null;
		}
	}

	/**
	 * 実行します。
	 * @param args
	 * @throws SQLException
	 */
	public void execute(String[] args)
		throws SQLException {
		String command = args[0];
		if("select".equals(command)) {
			executeSelect();
		}else if("insert".equals(command)) {
			executeInsert(args[1], args[2], args[3]);
		}else if("update".equals(command)) {
			executeUpdate(args[1], args[2], args[3]);
		}else if("delete".equals(command)) {
			executeDelete(args[1]);
		}
	}

	/**
	 * SELECT処理を実行します。
	 */
	private void executeSelect()
		throws SQLException{
		ResultSet resultSet = _statement.executeQuery("SELECT * FROM " + TABLE_NAME);
		try{
			boolean br = resultSet.first();
			if(br == false) {
				return;
			}
			do{
				String id = resultSet.getString("kyoten_code");
				String name = resultSet.getString("kyoten_name");
				String password = resultSet.getString("kyoten_kbn");

				System.out.println("kyoten_code: " + id + ", kyoten_name: " + name + ", kyoten_kbn: " + password);
			}while(resultSet.next());
		}finally{
			resultSet.close();
		}
	}

	/**
	 * INSERT処理を実行します。
	 * @param id
	 * @param name
	 * @param password
	 */
	private void executeInsert(String id, String name, String password)
		throws SQLException{
		// SQL文を発行
		int updateCount = _statement.executeUpdate("INSERT INTO " + TABLE_NAME + " (kyoten_code,kyoten_name,kyoten_kbn) VALUES ('"+id+"','"+name+"','"+password+"')");
		System.out.println("Insert: " + updateCount);
	}

	/**
	 * UPDATE処理を実行します。
	 * @param id
	 * @param name
	 * @param password
	 */
	private void executeUpdate(String id, String name, String password)
		throws SQLException{
		// SQL文を発行
		int updateCount = _statement.executeUpdate("UPDATE " + TABLE_NAME + " SET kyoten_name='"+name+"', kyoten_kbn='"+password+"' WHERE kyoten_code='" + id + "'");
		System.out.println("Update: " + updateCount);
	}

	/**
	 * DELETE処理を実行します。
	 * @param id
	 */
	private void executeDelete(String id)
		throws SQLException{
		// SQL文を発行
		int updateCount = _statement.executeUpdate("DELETE FROM " + TABLE_NAME + " WHERE kyoten_code='" + id + "'");
		System.out.println("Delete: " + updateCount);
	}

}
