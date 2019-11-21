/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 *
 * Класс SPRING BEAN
 */

package ru.yakimov;

import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.*;
import ru.yakimov.Jobs.DeleteFromTableJob;
import ru.yakimov.Jobs.ExportSqoopDirToDbJob;
import ru.yakimov.Jobs.ImportSqoopDbToDirJob;
import ru.yakimov.Jobs.LoadToHiveFromDirs;

import java.io.File;
import java.io.IOException;

//@ComponentScan (basePackages = "ru.yakimov")
@ImportResource(locations = "classpath:configBean.xml")
@Configuration
public class JobContextConfiguration {





    @Bean
    @Scope("prototype")
    public ImportSqoopDbToDirJob loadImportSqoopDbToDirJob(){
        return new ImportSqoopDbToDirJob();
    }

    @Bean
    public FileSystem loadFileSystem() throws IOException {
        return FileSystem.get(loadSpark().sparkContext().hadoopConfiguration());
    }

//    @Bean
//    public AppConfiguration loadAppConfig() throws FileNotFoundException, XMLStreamException {
//        return AppXmlLoader.readConfigApp(BootProcessMain.CONF_FILE_PATH);
//    }


    @Bean
    public SparkSession loadSpark(){
        String warehouseLocation = new File("./metastore_db").getAbsolutePath();
        SparkSession spark = SparkSession
                .builder()
                .appName("DataLoader")
                .config("spark.master", "local")
                .config ("hive.exec.dynamic.partition", "true")
                .config ("hive.exec.dynamic.partition.mode", "nonstrict")
                .config("hive.txn.manager", "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager")
                .config("hive.support.concurrency", "true")
                .config("hive.enforce.bucketing", "true")
//                .config("hive.metastore.uris", "jdbc:mysql://localhost:3306/metastore?serverTimezone=UTC&zeroDateTimeBehavior=CONVERT_TO_NULL")
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .config("spark.hadoop.fs.default.name", "hdfs://localhost:8020")
                .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:8020")
                .config("spark.hadoop.fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName())
                .config("spark.hadoop.fs.hdfs.server", org.apache.hadoop.hdfs.server.namenode.NameNode.class.getName())
                .config("spark.hadoop.conf", org.apache.hadoop.hdfs.HdfsConfiguration.class.getName())
                .enableHiveSupport()
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        return spark;
    }

    @Bean
    @Scope("prototype")
    public ExportSqoopDirToDbJob loadExportSqoopDirToDB(){
        return new ExportSqoopDirToDbJob();
    }

    @Bean
    @Scope("prototype")
    public LoadToHiveFromDirs loadJoinAnyBaseInOne(){
        return new LoadToHiveFromDirs();
    }

    @Bean
    @Scope("prototype")
    public DeleteFromTableJob loadDeleteFromTransactionTable(){
        return new DeleteFromTableJob();
    }





}
