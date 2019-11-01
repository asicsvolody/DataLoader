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

import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.StructType;
import ru.yakimov.Assets;
import ru.yakimov.MySqlDB.Log;
import ru.yakimov.config.JobConfiguration;

import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;


public class HiveUtils {

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
        SparkSession spark = Assets.getInstance().getSpark();
        String databaseTo = jobConf.getDbConfiguration().getSchema();
        String tableTo = jobConf.getDbConfiguration().getTable();

        Log.write(jobConf, "Read table columns");

        List<String> tableCols = spark
                .sql(String.format("DESC %s.%s",databaseTo,tableTo))
                .toJavaRDD()
                .map(row -> row.getString(0).trim())
                .filter(v -> !v.startsWith("#"))
                .collect();

        Log.write(jobConf, tableCols.toString());

        Log.write(jobConf, "Spark read data from dir "+ dirFrom);

        Dataset<Row> data = spark.read()
                .parquet(dirFrom + Assets.SEPARATOR + "*.parquet");

        data.createOrReplaceTempView("tmp_table");

        data.show();

        String usualCols = String.join(","
                , LoaderUtils.getColsAndNullNoPartitions(
                        tableCols,
                        data.schema().fieldNames(),
                        jobConf.getPartitions()
                )

        );

        Log.write(jobConf, usualCols);

        String partitionsCols= String.join(", ",LoaderUtils.getColumnNameOnly(jobConf.getPartitions()));

        Log.write(jobConf, "Write data to Hive Table");

        String hiveScript = String.format(
                "INSERT INTO %s.%s PARTITION(%s) SELECT %s,%s FROM tmp_table"
                , databaseTo
                ,tableTo
                ,partitionsCols
                ,usualCols
                ,partitionsCols
        );

        Log.write(jobConf, hiveScript);

        spark.sql(hiveScript);
    }

    /**
     * Создание Hive таблицы из массива полей
     *
     * @param jConfig
     * @param columnsArr
     * @throws Exception
     */
    public static synchronized void createHiveTable(JobConfiguration jConfig, String[] columnsArr, TableType type) throws Exception {
        SparkSession spark = Assets.getInstance().getSpark();

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

        HdfsUtils.deleteDirWithLog(jConfig, Assets.SEPARATOR +schema + Assets.SEPARATOR+ table);


        String cols = String.join(", ", LoaderUtils.getUsualColumns(columnsArr, jConfig.getPartitions()));

        String partitions = String.join(",", jConfig.getPartitions());

        System.out.println(cols);

        String hiveScript = String.format( getCreateScript(type)
                ,schema, table, cols, partitions, schema, table );

        Log.write(jConfig, hiveScript);

        spark.sql(hiveScript);

    }


    public static synchronized void createTransactionalHiveTable(JobConfiguration jConfig, StructType type) throws Exception {

        createHiveTable(jConfig, LoaderUtils.getFormattingCols(type).toArray(new String[0]), TableType.TRANSACTIONAL);

    }


    public static synchronized void createTransactionalHiveTable(JobConfiguration jConfig, Set<String> listColumns) throws Exception {
        createHiveTable(jConfig, listColumns.toArray(new String[0]), TableType.TRANSACTIONAL);
    }


    public static synchronized void createExternalHiveTable(JobConfiguration jConfig, StructType type) throws Exception {

        createHiveTable(jConfig, LoaderUtils.getFormattingCols(type).toArray(new String[0]), TableType.EXTERNAL);

    }

    public static synchronized void createExternalHiveTable(JobConfiguration jConfig, Set<String> listColumns) throws Exception {
        createHiveTable(jConfig, listColumns.toArray(new String[0]), TableType.EXTERNAL);
    }

    public static synchronized void deleteFromHiveTable(JobConfiguration jConf, String dir) throws Exception {
        SparkSession spark = Assets.getInstance().getSpark();

        String schema = jConf.getDbConfiguration().getSchema();
        String table = jConf.getDbConfiguration().getTable();

        String partitionsCols= String.join(", ",LoaderUtils.getColumnNameOnly(jConf.getPartitions()));


        Log.write(jConf, "Spark read new data from table");

        String sparkScript = String.format("SELECT * FROM parquet.`%s/*.parquet`",dir);

        Log.write(jConf, sparkScript);

        Dataset<Row> data = spark.sql(sparkScript);

        if(!LoaderUtils.schemaContainsAll(data.schema(), jConf.getDbConfiguration().getPrimaryKeys())){
            Log.writeExceptionAndGet(jConf, "Primary keys from main table not exist");
        }
        data.show();

        data.createOrReplaceTempView("tmp_table");

        String hiveScript = String.format("INSERT OVERWRITE TABLE %s.%s PARTITION(%s) SELECT t.* FROM %s.%s t LEFT JOIN tmp_table ON %s WHERE %s"
                ,schema
                ,table
                ,partitionsCols
                ,schema
                ,table
                , getSelectPrimary(jConf.getDbConfiguration().getPrimaryKeys())
                ,getPrimaryIsNull(jConf.getDbConfiguration().getPrimaryKeys())
        );

        Log.write(jConf, hiveScript);

        Log.write(jConf, String.format("Spark delete data from table %s.%s"
                ,jConf.getDbConfiguration().getSchema()
                ,jConf.getDbConfiguration().getTable()));

//        spark.sql("SELECT t.* FROM jointSchema.jointTable t LEFT JOIN tmp_table ON t.user_id=tmp_table.user_id AND t.user_age=tmp_table.user_age WHERE tmp_table.user_id IS NULL AND tmp_table.user_age IS NULL").sort("t.user_id").show();
        spark.sql(hiveScript);

    }

    private static String getSelectPrimary(List<String> primaryKeys) {
        List<String> lines = new ArrayList<>();
        for (String primaryKey : primaryKeys) {
            lines.add(String.format("t.%s=tmp_table.%s", primaryKey, primaryKey));
        }
        return String.join(" AND ",lines.toArray(new String[0]));
    }

    private static String getPrimaryIsNull(List<String> primaryKeys){
        List<String> lines = new ArrayList<>();
        for (String primaryKey : primaryKeys) {
            lines.add(String.format("tmp_table.%s IS NULL", primaryKey));
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
