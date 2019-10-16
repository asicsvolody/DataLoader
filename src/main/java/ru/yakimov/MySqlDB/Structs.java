/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 */

package ru.yakimov.MySqlDB;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;

public class Structs {

    public static synchronized void write(String schema, String table, String json) throws SQLException {

        String sql = "INSERT INTO STRUCT_TYPES(STRUCT_TYPES_SCHEMA,STRUCT_TYPES_TABLE,STRUCT_TYPES_JSON) values (?, ?, ?)";
        PreparedStatement ps = MySqlDb.getConnection().prepareStatement(sql);
        ps.setString(1, schema);
        ps.setString(2, table);
        ps.setString(3, json);
        ps.executeUpdate();
        ps.close();
        MySqlDb.getConnection().commit();

    }
}
