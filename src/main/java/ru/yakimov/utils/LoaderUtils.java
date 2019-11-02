/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 *
 * Класс статических утилит программы
 */

package ru.yakimov.utils;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import ru.yakimov.MySqlDB.Log;
import ru.yakimov.config.JobConfiguration;

import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.spark.sql.types.DataTypes.*;

public class LoaderUtils {

    /**
     * Метод выборки полей не партицирования
     *
     * @param type
     * @param partition
     * @return
     */
    public static List<String> getUsualColumns(StructType type, List<String> partition){
        return getUsualColumns(Arrays.asList(type.fieldNames()), partition);
    }

    /**
     * Метод выборки полей не партицирования
     *
     * @param cols
     * @param partitions
     * @return
     */
    public static List<String> getUsualColumns(List<String> cols, List<String> partitions) {
        return cols.stream()
                .filter(v ->!partitions.contains(v))
                .collect(Collectors.toList());
    }

//    /**
//     * Метод проверяет партицированное ли это поле
//     * @param name
//     * @param partitions
//     * @return
//     */
//    public static boolean isPartition(String name, List<String> partitions) {
//        return partitions.contains(name);
//    }

    /**
     * Метод возвращающий коллекциб String представления полей с ттпами Hive
     *
     * @param type
     * @return
     */
    public static List<String> getFormattingCols(StructType type) {
        return Arrays.stream(type.fields())
                .map(v -> v.name().toLowerCase() + " " + convertSparkTypeToHiveTypeStr(v.dataType()))
                .collect(Collectors.toList());
    }

    /**
     * Метод конвертации  типа в Hive type
     *
     * @param type
     * @return
     */
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

    /**
     * Метод генерации исключения если коллекция не имеет одно из полей партиционирования
     *
     * @param columnsList
     * @param jobConfig
     * @param dir
     * @throws Exception
     */
    public static void checkPartitions(List<String> columnsList, JobConfiguration jobConfig, String dir) throws Exception {
        Optional<String> colOpt = jobConfig.getPartitions()
                .stream()
                .filter(v -> !columnsList.contains(v))
                .findAny();
        if(colOpt.isPresent())
            Log.writeExceptionAndGet(jobConfig, "Data from dir: "+ dir + " have not column "+ colOpt.get());
    }

    /**
     * Метод возврящающий массив имен полей с заполнением null недостающими под формат hive таблицы
     *
     * @param tableColumnNames
     * @param dataColumnNames
     * @param partitions
     * @return
     */
    public static List<String> getColsAndNullNoPartitions(List<String> tableColumnNames, List<String> dataColumnNames,  List<String> partitions) {
        return tableColumnNames.stream()
                .filter(v -> !partitions.contains(v))
                .map(v -> (dataColumnNames.contains(v)) ? v : "null as " + v)
                .collect(Collectors.toList());
    }

    /**
     * Метод отсекающий типы ищы String представления поля
     *
     * @param colArr
     * @return
     */
    public static List<String> getColumnNameOnly(List<String> colArr){
        return colArr.stream().map(v -> v.split("\\s")[0]).collect(Collectors.toList());

    }

    public static boolean schemaContainsAll(StructType schema, ArrayList<String> primaryKeys) {
        return Arrays.asList(schema.fieldNames()).containsAll(primaryKeys);
    }

    public static void  deleteDirs(JobConfiguration jConf, List<String> dirs) throws SQLException, IOException, XMLStreamException {
        for (String dir : dirs) {
            HdfsUtils.deleteDirWithLog(jConf, dir);
        }
    }
}
