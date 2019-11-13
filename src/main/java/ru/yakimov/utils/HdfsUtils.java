/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 *
 * Статические методы работы с HADOOP FS
 */

package ru.yakimov.utils;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import ru.yakimov.BootProcessMain;
import ru.yakimov.config.JobConfiguration;
import ru.yakimov.MySqlDB.Log;

import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.sql.SQLException;

public class HdfsUtils {

    /**
     * Метод удаления директории с данными из HDFS
     *
     * @param path
     * @return
     * @throws XMLStreamException
     * @throws IOException
     * @throws SQLException
     */
    public static synchronized boolean deleteFromHadoop(String path) throws XMLStreamException, IOException, SQLException {
        FileSystem fs = BootProcessMain.CONTEXT.getBean(FileSystem.class);
        Path file = new Path(path);
        if(fs.exists(file)){
            fs.delete(file, true);
            return true;
        }
        return false;
    }

    /**
     * Метод удаления директории с данными из HDFS с логированием
     *
     * @param config
     * @param hdfsDir
     * @throws SQLException
     * @throws IOException
     * @throws XMLStreamException
     */
    public static synchronized void deleteDirWithLog(JobConfiguration config,  String hdfsDir) throws SQLException, IOException, XMLStreamException {
        if(deleteFromHadoop(hdfsDir)){
            Log.write(config, "Delete "+ hdfsDir );
        }
    }

    /**
     * Метод переноса файла из локальной FS в HDFS
     * @param pathStr
     * @param str
     * @return
     * @throws XMLStreamException
     * @throws IOException
     * @throws SQLException
     */
    public static synchronized boolean writeToHdfs(String pathStr, String str) throws XMLStreamException, IOException, SQLException {

        FileSystem fs = BootProcessMain.CONTEXT.getBean(FileSystem.class);
        Path path = new Path(pathStr);
        deleteFromHadoop(pathStr);

        try(FSDataOutputStream out = fs.create(path)) {
            out.writeUTF(str);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;

    }

    /**
     * Метод чтения данных из файла HDFS
     *
     * @param pathStr
     * @return
     * @throws XMLStreamException
     * @throws IOException
     * @throws SQLException
     */
    public static synchronized String readFromHdfs(String pathStr) throws XMLStreamException, IOException, SQLException {
        FileSystem fs = BootProcessMain.CONTEXT.getBean(FileSystem.class);
        Path path = new Path(pathStr);
        String res = null;
        try(FSDataInputStream in = fs.open(path)){
            res = in.readUTF();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return res;
    }

}
