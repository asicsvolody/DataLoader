/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 *
 * Класс статическич Hive методов
 */

package ru.yakimov.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import org.apache.spark.sql.types.StructType;
import ru.yakimov.BootProcessMain;
import ru.yakimov.MySqlDB.Log;
import ru.yakimov.config.JobConfiguration;

import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;


public class HiveUtils implements Serializable {

    private enum TableType{
        EXTERNAL, TRANSACTIONAL
    }

    /**
     * Динамический insert в таблицу hive c автоматическим заполнением недостающий полей значениями null
     *
     * @param jobConf
     * @param dirFrom
     * @throws Exception
     */
    public static void insetToHiveTable(JobConfiguration jobConf, String dirFrom ) throws Exception {
        SparkSession spark = BootProcessMain.CONTEXT.getBean(SparkSession.class);
        String databaseTo = jobConf.getDbConfiguration().getSchema();
        String tableTo = jobConf.getDbConfiguration().getTable();

        String tmpTableName = "tmp_"+dirFrom.replace("/","_");

        Log.write(jobConf, "Read table columns");

        List<String> tableCols = getColumnsHiveTable(databaseTo, tableTo);

        Log.write(jobConf, tableCols.toString());

        Log.write(jobConf, "Spark read data from dir "+ dirFrom);

        String sparkScript = String.format("CREATE TEMPORARY VIEW %s USING parquet OPTIONS(path '%s')",tmpTableName, dirFrom);

        Log.write(jobConf, sparkScript);

        spark.sql(sparkScript);

        Dataset<Row> data = spark.sql("SELECT * FROM "+tmpTableName);

        data.show();

        String usualCols = String.join(","
                , LoaderUtils.getColsAndNullNoPartitions(
                        tableCols,
                        Arrays.asList(data.schema().fieldNames()),
                        LoaderUtils.getColumnNameOnly(jobConf.getPartitions())
                )

        );

        Log.write(jobConf, usualCols);

        String partitionsCols= String.join(", ",LoaderUtils.getColumnNameOnly(jobConf.getPartitions()));

        Log.write(jobConf, "Write data to Hive Table");

        String hiveScript = String.format(
                "INSERT INTO %s.%s PARTITION(%s) SELECT %s,%s FROM %s"
                , databaseTo
                ,tableTo
                ,partitionsCols
                ,usualCols
                ,partitionsCols
                ,tmpTableName
        );

        Log.write(jobConf, hiveScript);

        spark.sql(hiveScript);
    }

    private static List<String> getColumnsHiveTable(String databaseTo, String tableTo) throws XMLStreamException, IOException, SQLException {
        return BootProcessMain.CONTEXT.getBean(SparkSession.class)
                .sql(String.format("DESC %s.%s",databaseTo,tableTo))
                .toJavaRDD()
                .map(row -> row.getString(0)
                        .trim())
                .filter(v -> !v.startsWith("#"))
                .collect();
    }

    private static List<String> getUsualColumnsHiveTable(String databaseTo, String tableTo, List<String> partitionColumns) throws XMLStreamException, IOException, SQLException {
        return getColumnsHiveTable(databaseTo,tableTo).stream().filter(v -> !partitionColumns.contains(v)).collect(Collectors.toList());
    }

    /**
     * Создание Hive таблицы из массива полей
     *
     * @param jConfig
     * @param columnsArr
     * @throws Exception
     */
    public static synchronized void createHiveTable(JobConfiguration jConfig, List<String> columnsArr, TableType type) throws Exception {
        SparkSession spark = BootProcessMain.CONTEXT.getBean(SparkSession.class);

        String schema = jConfig.getDbConfiguration().getSchema();

        if(schema == null){
            throw new Exception("There is not schema name");
        }

        String table = jConfig.getDbConfiguration().getTable();

        if(table == null){
            throw new Exception("There is not table name");
        }

        Log.write(jConfig, "Creating database: "+ schema);

        spark.sql(String.format("CREATE DATABASE IF NOT EXISTS %s LOCATION '/%s'",schema, schema));

        Log.write(jConfig, "Creating table: " +table);

        Log.write(jConfig, "Delete old table if exist");

        spark.sql(String.format("DROP TABLE IF EXISTS %s.%s", schema, jConfig.getDbConfiguration().getTable()));

        HdfsUtils.deleteDirWithLog(jConfig, BootProcessMain.SEPARATOR +schema + BootProcessMain.SEPARATOR+ table);


        String cols = String.join(", ", LoaderUtils.getUsualColumns(columnsArr, jConfig.getPartitions()));

        String partitions = String.join(",", jConfig.getPartitions());

        System.out.println(cols);

        String hiveScript = String.format( getCreateScript(type)
                ,schema, table, cols, partitions, schema, table );

        Log.write(jConfig, hiveScript);

        spark.sql(hiveScript);

    }


    public static synchronized void createTransactionalHiveTable(JobConfiguration jConfig, StructType type) throws Exception {

        createHiveTable(jConfig, LoaderUtils.getFormattingCols(type), TableType.TRANSACTIONAL);

    }


    public static synchronized void createTransactionalHiveTable(JobConfiguration jConfig, Set<String> listColumns) throws Exception {
        createHiveTable(jConfig, new ArrayList<>(listColumns), TableType.TRANSACTIONAL);
    }


    public static synchronized void createExternalHiveTable(JobConfiguration jConfig, StructType type) throws Exception {

        createHiveTable(jConfig, LoaderUtils.getFormattingCols(type), TableType.EXTERNAL);

    }

    public static synchronized void createExternalHiveTable(JobConfiguration jConfig, Set<String> listColumns) throws Exception {
        createHiveTable(jConfig, new ArrayList<>(listColumns), TableType.EXTERNAL);
    }

    public static synchronized void deleteFromHiveTable(JobConfiguration jobConf, String dir) throws Exception {
        SparkSession spark = BootProcessMain.CONTEXT.getBean(SparkSession.class);

        String schema = jobConf.getDbConfiguration().getSchema();
        String table = jobConf.getDbConfiguration().getTable();
        String tmpTableName = "tmp_"+jobConf.getJobName();


        List<String> partitionsColsList = LoaderUtils.getColumnNameOnly(jobConf.getPartitions());

        String partitionsCols= String.join(", ",partitionsColsList);

        Log.write(jobConf, "Spark read new data from table");

        String sparkScript = String.format("CREATE TEMPORARY VIEW %s USING parquet OPTIONS(path '%s')",tmpTableName,dir);

        Log.write(jobConf, sparkScript);

        spark.sql(sparkScript);

        Dataset<Row> data = spark.sql("SELECT * FROM "+tmpTableName);

        if(!LoaderUtils.schemaContainsAll(data.schema(), jobConf.getDbConfiguration().getPrimaryKeys())){
            Log.writeExceptionAndGet(jobConf, "Primary keys from main table not exist");
        }
        data.show();

       List<Row> usingPartitionsRows = spark.sql(String.format("SELECT %s FROM %s"
                , partitionsCols, tmpTableName))
               .distinct()
               .toJavaRDD()
               .collect();

       List<String[]> partitions = LoaderUtils.getPartitionData(usingPartitionsRows, partitionsColsList);

       String usualColumnsWithT = getUsualColumnsHiveTable(schema, table, partitionsColsList)
               .stream()
               .map(v -> "t."+v)
               .collect(Collectors.joining(","));



        for (String[] partition : partitions) {
            String hiveScript = String.format("INSERT OVERWRITE TABLE %s.%s PARTITION(%s) SELECT %s FROM %s.%s t LEFT JOIN %s ON %s WHERE %s AND %s"
                    ,schema
                    ,table
                    ,String.join(",",partition)
                    ,usualColumnsWithT
                    ,schema
                    ,table
                    ,tmpTableName
                    , getSelectPrimary(jobConf.getDbConfiguration().getPrimaryKeys(), tmpTableName)
                    , LoaderUtils.getStringWithT(" AND ",partition)
                    ,getPrimaryIsNull(jobConf.getDbConfiguration().getPrimaryKeys(), tmpTableName)
            );

            Log.write(jobConf, hiveScript);

            Log.write(jobConf, String.format("Spark delete data from table %s.%s in partition %s"
                    ,jobConf.getDbConfiguration().getSchema()
                    ,jobConf.getDbConfiguration().getTable()
                    ,String.join(",",partition)
            ));

            spark.sql(hiveScript);

        }

    }

    private static String getSelectPrimary(List<String> primaryKeys, String tmpTable) {
        List<String> lines = new ArrayList<>();
        for (String primaryKey : primaryKeys) {
            lines.add(String.format("t.%s=%s.%s", primaryKey,tmpTable, primaryKey));
        }
        return String.join(" AND ",lines.toArray(new String[0]));
    }

    private static String getPrimaryIsNull(List<String> primaryKeys, String tmpTable){
        List<String> lines = new ArrayList<>();
        for (String primaryKey : primaryKeys) {
            lines.add(String.format("%s.%s IS NULL",tmpTable, primaryKey));
        }
        return String.join(" AND ",lines.toArray(new String[0]));
    }

    private static String getCreateScript(TableType type){
        switch (type){
            case EXTERNAL:
                return "CREATE EXTERNAL TABLE IF NOT EXISTS %s.%s \n" +
                     " (%s) \n" +
                    " PARTITIONED BY (%s)\n" +
                    " STORED AS PARQUET \n" +
                    " LOCATION '/%s/%s' ";

            case TRANSACTIONAL:
                return "CREATE TABLE IF NOT EXISTS %s.%s \n" +
                         " (%s) \n" +
                        " PARTITIONED BY (%s)\n" +
                        " STORED AS ORC \n" +
                        " LOCATION '/%s/%s' " +
                        "TBLPROPERTIES ('transactional'='true')";
        }
        return null;
    }

}
