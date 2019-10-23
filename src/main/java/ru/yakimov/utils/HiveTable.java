/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 */

package ru.yakimov.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import ru.yakimov.Assets;
import ru.yakimov.MySqlDB.Log;
import ru.yakimov.config.JobConfiguration;

import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.types.DataTypes.*;

public class HiveTable {


    public static void createHiveTable(JobConfiguration jConfig, StructType type) throws Exception {
        SparkSession spark = Assets.getInstance().getSpark();
        String schema = jConfig.getDbConfiguration().getSchema();

        if(schema == null){
            throw new Exception("There is not schema name");
        }

        Log.write(jConfig, "Creating database and table");

        spark.sql(String.format("DROP TABLE IF EXISTS %s.%s", schema, jConfig.getDbConfiguration().getTable()));

        spark.sql(String.format("CREATE DATABASE IF NOT EXISTS %s LOCATION '/%s'",schema, schema));


        String table = jConfig.getDbConfiguration().getTable();

        if(table == null){
            throw new Exception("There is not table name");
        }
        String partitions = getPartitionStr(type, jConfig.getPartitions());

        String cols = getUsualFieldsStr(type, jConfig.getPartitions());

        spark.sql(String.format("CREATE EXTERNAL TABLE IF NOT EXISTS %s.%s \n" +
                " (%s) \n" +
                " PARTITIONED BY (%s)\n" +
                " STORED AS PARQUET \n" +
                " LOCATION '/%s/%s' ",schema, table, cols, partitions, schema, table ));

    }

    public static String getPartitionStr(StructType type, String[] partitions) {

        List<String> list = new ArrayList<>();
        for (String partition : partitions) {
            StructField field = type.apply(partition);
            if(field == null){
                throw new NullPointerException("There is not field for partition: "+ partition);
            }
            list.add(field.name().toLowerCase()+" "+convertSparkTypeToHiveTypeStr(field.dataType()));
        }

        return String.join(",",list.toArray(new String[0]));
    }

    private static String getUsualFieldsStr(StructType type, String[] partitions) {
        List<String> list = new ArrayList<>();
        for (StructField field : type.fields()) {
            if(!LoaderUtils.isPartition(field.name(), partitions)){
                list.add(field.name().toLowerCase()+" "+convertSparkTypeToHiveTypeStr(field.dataType()));
            }
        }
        return String.join(",",list.toArray(new String[0]));
    }


    private static String convertSparkTypeToHiveTypeStr(DataType type){
        if (BinaryType.equals(type)) {
            return "binary";
        } else if (BooleanType.equals(type)) {
            return "boolean";
        } else if (ByteType.equals(type)) {
            return "tinyint";
        } else if (CalendarIntervalType.equals(type)) {
            return "interval";
        } else if (DateType.equals(type)) {
            return "date";
        } else if (DoubleType.equals(type)) {
            return "double";
        } else if (FloatType.equals(type)) {
            return "float";
        } else if (IntegerType.equals(type)) {
            return "int";
        } else if (LongType.equals(type)) {
            return "bigint";
        } else if (NullType.equals(type)) {
            return "null";
        } else if (ShortType.equals(type)) {
            return "smallint";
        } else if (StringType.equals(type)) {
            return "string";
        } else if (TimestampType.equals(type)) {
            return "timestamp";
        }
        return null;
    }



}
