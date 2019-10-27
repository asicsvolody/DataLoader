/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 * Класс для обслуживания подключения к БД Mysql
 * БД Mysql хранит информацию необходимссую для работы, логи
 * использует общее статическое подключение (singleton)
 */

package ru.yakimov.MySqlDB;

import ru.yakimov.config.DBConfiguration;

import java.sql.*;
import java.util.ArrayList;

public class MySqlDb {

    private DBConfiguration config;

    public static String dbUrl;

    public static String dbUser;

    public static String dbPass;

    private static Connection conn;

    private final static String JDBC = "com.mysql.jdbc.Driver";

    public static Boolean dbDebug = false;

    public static boolean isDbConnected() {
        final String CHECK_SQL_QUERY = "SELECT 1";
        boolean isConnected = false;
        try {
            conn.prepareStatement(CHECK_SQL_QUERY).execute();
            isConnected = true;
        }
        catch (SQLException | NullPointerException e)
        {
            if (dbDebug)
            {
                System.err.println("Connection is closed!");
                System.err.println(e.getMessage());

            }
        }
        return isConnected;
    }

    public MySqlDb() {
    }

    /**
     * Метод подключени к базе Mysql
     * @return
     * @throws SQLException
     */
    public static Connection getConnection() throws SQLException {

        if (conn == null || (conn != null && !isDbConnected())) {
            try {
                Class.forName(JDBC);
            }catch (ClassNotFoundException e) {
                System.out.println("Ошибка подключения к БД MySQL!");
                throw new SQLException("Ошибка подключения к БД MySQL!");
            }
            conn = DriverManager.getConnection(dbUrl, dbUser, dbPass);
            conn.setAutoCommit(false);

            // Установка параметров
            Statement stmt = conn.createStatement();
            stmt.executeUpdate("set session wait_timeout = 201600");
            stmt.close();

            return conn;
        }
        return conn;
    }

    /**
     * Получить результат выполнения запроса sql(первая запись первое поле)
     * @param sql
     * @return
     * @throws SQLException
     */
    public static String getSqlResult(String sql) throws SQLException {
        if (dbDebug) {
            System.err.println(sql);
        }
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        String res = "";
        while (rs.next()) {
            res = rs.getString(1);
        }
        rs.close();
        stmt.close();
        return res;
    }

    /**
     * Выполнить sql команду
     * @param sql
     * @throws SQLException
     */
    public static void execSQL(String sql) throws SQLException {
        if (dbDebug) {
            System.err.println(sql);
        }
        Statement stmt = conn.createStatement();
        stmt.executeUpdate(sql);
        stmt.close();
    }

    /**
     * Получить результат выполнения запроса sql (коллекция из записей первой
     * колонки результата)
     * @param sql
     * @return
     * @throws SQLException
     */
    public static ArrayList<String> getSqlResults(String sql)
            throws SQLException {
        if (dbDebug) {
            System.err.println(sql);
        }
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        ArrayList<String> res = new ArrayList<String>();
        while (rs.next()) {

            res.add(rs.getString(1));
        }
        rs.close();
        stmt.close();
        return res;
    }

    /**
     * Инициализация соединения (singleton)
     *
     * @param config
     * @return
     * @throws SQLException
     */
    public static Connection initConnection(DBConfiguration config) throws SQLException {
        return initConnection(config, false);
    }

    /**
     * Инициализация соединения (singleton)
     *
     * @param config
     * @param debug
     * @return
     * @throws SQLException
     */
    public static Connection initConnection(DBConfiguration config, Boolean debug) throws SQLException {
        dbUrl = String.format("jdbc:mysql://%s:%s/%s?serverTimezone=UTC&zeroDateTimeBehavior=CONVERT_TO_NULL",config.getHost(),config.getPort(),config.getSchema());
        dbUser = config.getUser();
        dbPass = config.getPassword();
        if (debug == null) {
            dbDebug = false;
        }
        dbDebug = debug;

        return getConnection();
    }


    /**
     * Закрыть соединение
     */
    public static void closeConnection() {
        if (conn == null) {
            return;
        }
        try {
            conn.commit();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }






}
