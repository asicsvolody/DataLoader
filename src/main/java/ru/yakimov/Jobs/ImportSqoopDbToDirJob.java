/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 *
 * Класс задания импорта базы в директорию hdfs, является компонентом Spring
 */

package ru.yakimov.Jobs;

import org.springframework.stereotype.Component;
import ru.yakimov.config.DBConfiguration;
import ru.yakimov.LogDB.Log;
import ru.yakimov.utils.HdfsUtils;
import ru.yakimov.utils.SqoopUtils;

import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.sql.SQLException;


@Component
public class ImportSqoopDbToDirJob extends Job {

    @Override
    public Integer call() throws SQLException, IOException, XMLStreamException, InterruptedException {
        Log.write(jobConfig, "Sqoop get task import");

        DBConfiguration dbConfig = jobConfig.getDbConfiguration();

        Log.write(jobConfig, "Start creating password file");

        SqoopUtils.createPassword(dbConfig.getPassword(), jobConfig);

        HdfsUtils.deleteDirWithLog(jobConfig, jobConfig.getDirTo());

        Log.write(jobConfig, "Sqoop run process");
        Process process = Runtime.getRuntime().exec(String.format("sqoop import " +
                        "--connect jdbc:mysql://%s:%s/%s?serverTimezone=UTC&zeroDateTimeBehavior=CONVERT_TO_NULL " +
                        "--username %s " +
                        "--password-file %s " +
                        "--table %s " +
                        "--target-dir %s " +
                        "--split-by %s " +
                        "--as-parquetfile"
                ,dbConfig.getHost()
                ,dbConfig.getPort()
                ,dbConfig.getSchema()
                ,dbConfig.getUser()
                ,SqoopUtils.getHadoopPasswordPath(jobConfig.getJobName())
                ,dbConfig.getTable()
                , jobConfig.getDirTo()
                ,dbConfig.getPrimaryKeys().get(0)
        ));

        Log.write(jobConfig, "Waiting of end sqoop process");
        process.waitFor();

        SqoopUtils.writeProcessMessageStream(process, jobConfig);

        SqoopUtils.writeResSqoop(jobConfig, process.exitValue());

        Log.write(jobConfig, "Delete password: " + SqoopUtils.getHadoopPasswordPath(jobConfig.getJobName() ));

        HdfsUtils.deleteDirWithLog(jobConfig, SqoopUtils.getHadoopPasswordPath(jobConfig.getJobName()));

        return process.exitValue();
    }
}
