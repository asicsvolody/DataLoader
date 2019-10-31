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

import static org.apache.spark.sql.types.DataTypes.*;

public class LoaderUtils {

    /**
     * Метод выборки полей не партицирования
     *
     * @param type
     * @param partition
     * @return
     */
    public static String[] getUsualColumns(StructType type, String [] partition){
        List<String> resCols = new ArrayList<>();
        for (String field : type.fieldNames()) {
            if(!isPartition(field, partition)){
                resCols.add(field);
            }
        }
        return resCols.toArray(new String[0]);
    }

    /**
     * Метод выборки полей не партицирования
     *
     * @param colsArr
     * @param partitions
     * @return
     */
    public static String[] getUsualColumns(String[] colsArr, String[] partitions) {
        List<String> resList = new ArrayList<>();
        for (String col : colsArr) {
            if(!isPartition(col, partitions )){
                resList.add(col);
            }
        }
        return resList.toArray(new String[0]);
    }

    /**
     * Метод проверяет партицированное ли это поле
     * @param name
     * @param partitions
     * @return
     */
    public static boolean isPartition(String name, String[] partitions) {
        for (String partition : partitions) {
            if(name.toLowerCase().equals(partition.toLowerCase()))
                return true;
        }
        return false;
    }

    /**
     * Метод возвращающий коллекциб String представления полей с ттпами Hive
     *
     * @param type
     * @return
     */
    public static List<String> getFormattingCols(StructType type) {

        List<String> list = new ArrayList<>();
        for (StructField field : type.fields()) {
            list.add(field.name().toLowerCase() + " " + convertSparkTypeToHiveTypeStr(field.dataType()));
        }
        return list;
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
        for (String partition : jobConfig.getPartitions()) {
            if(!columnsList.contains(partition)){
                Log.writeExceptionAndGet(jobConfig, "Data from dir: "+ dir + " have not column "+ partition);
            }
        }
    }

    /**
     * Метод возврящающий массив имен полей с заполнением null недостающими под формат hive таблицы
     *
     * @param tableColumnNames
     * @param dataColumnNames
     * @param partitions
     * @return
     */
    public static String[] getColsAndNullNoPartitions(List<String> tableColumnNames, String[] dataColumnNames, String[] partitions) {

        List<String> partitionList = Arrays.asList(getColumnNameOnly(partitions));
        List<String> dataColumnNamesList = Arrays.asList(dataColumnNames);

        List<String> resList = new ArrayList<>();

        for (String tableColumnName : tableColumnNames) {
            if(!partitionList.contains(tableColumnName)){
                resList.add((dataColumnNamesList.contains(tableColumnName))?tableColumnName: "null as " + tableColumnName);
            }
        }
        return resList.toArray(new String[0]);
    }

    /**
     * Метод отсекающий типы ищы String представления поля
     *
     * @param colArr
     * @return
     */
    public static String[] getColumnNameOnly(String[] colArr){
        for (int i = 0; i <colArr.length ; i++) {
            colArr[i] = colArr[i].split(" ")[0];
        }
        return colArr;
    }

    public static boolean schemaContainsAll(StructType schema, ArrayList<String> primaryKeys) {
        List<String> dataColumns = Arrays.asList(schema.fieldNames());
        for (String primaryKey : primaryKeys) {
            if(!dataColumns.contains(primaryKey))
                return false;

        }
        return true;
    }

    public static void  deleteDirs(JobConfiguration jConf, String[] dirs) throws SQLException, IOException, XMLStreamException {
        for (String dir : dirs) {
            HdfsUtils.deleteDirWithLog(jConf, dir);
        }
    }
}
