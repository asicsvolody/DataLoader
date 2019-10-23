/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 */

package ru.yakimov;

import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import ru.yakimov.config.AppConfiguration;

import ru.yakimov.config.AppXmlLoader;
import ru.yakimov.config.JobXmlLoader;
import ru.yakimov.MySqlDB.Log;
import ru.yakimov.MySqlDB.MySqlDb;

import javax.xml.stream.XMLStreamException;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


public class Assets {

    public static final String SEPARATOR = "/";
    public static final String STRUCT_JSON_FILE = "struct.json";

    private final String CONF_FILE_PATH = "conf.xml";
    public static final String MAIN_PROS = JobXmlLoader.createNameWithData("SYSTEM_PROSES");
    private AppConfiguration conf;
    private final SparkSession spark;
    private final FileSystem fs;
    private final Runtime rt;

    private static Assets instance;

    public static Assets getInstance() throws XMLStreamException, IOException, SQLException {
        Assets localInstance = instance;
        if(localInstance == null){
            synchronized (Assets.class){
                localInstance = instance;
                if(localInstance == null){
                    localInstance = instance = new Assets();
                }
            }
        }
        return  localInstance;
    }


    private Assets() throws IOException, XMLStreamException, SQLException {


        this.conf = AppXmlLoader.readConfigApp(CONF_FILE_PATH);



        try {
            MySqlDb.initConnection(conf.getWorkSchema());
        } catch (Exception e) {
            e.printStackTrace();
            throw new SQLException("Exception connection to log database");
        }


        Log.writeRoot(MAIN_PROS, "System configuration have redden");

        Log.writeRoot(MAIN_PROS, "Log dataBase have connected");



        this.rt = Runtime.getRuntime();

        Log.writeRoot(MAIN_PROS, "System Runtime have gotten ");


        String warehouseLocation = new File("/usr/local/Cellar/hive/3.1.2/libexec").getAbsolutePath();

        spark = SparkSession
                .builder()
                .appName("SparkSqoopWork")
                .config("spark.master", "local")
                .config ("hive.exec.dynamic.partition", "true")
                .config ("hive.exec.dynamic.partition.mode", "nonstrict")
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .config("spark.hadoop.fs.default.name", "hdfs://localhost:8020")
                .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:8020")
                .config("spark.hadoop.fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName())
                .config("spark.hadoop.fs.hdfs.server", org.apache.hadoop.hdfs.server.namenode.NameNode.class.getName())
                .config("spark.hadoop.conf", org.apache.hadoop.hdfs.HdfsConfiguration.class.getName())
                .enableHiveSupport()
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        Log.writeRoot(MAIN_PROS, "Spark have configured ");

        try {
            this.fs = FileSystem.get(spark.sparkContext().hadoopConfiguration());
        } catch (IOException e) {
            e.printStackTrace();
            throw new IOException("Exception connection to Hadoop file system");
        }

        Log.writeRoot(MAIN_PROS, "Hadoop file system have gotten ");
        
    }

    public AppConfiguration getConf() {
        return conf;
    }

    public SparkSession getSpark() {

        return spark;
    }

    public FileSystem getFs() {
        return fs;
    }

    public Runtime getRt() {
        return rt;
    }

    public static void closeResources(){

        MySqlDb.closeConnection();

        instance = null;
    }

}
