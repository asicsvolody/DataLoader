/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 */

package ru.yakimov.utils;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import ru.yakimov.Assets;
import ru.yakimov.config.JobConfiguration;
import ru.yakimov.MySqlDB.Log;

import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.sql.SQLException;

public class HdfsUtils {

    public static synchronized boolean deleteFromHadoop(String path) throws XMLStreamException, IOException, SQLException {
        FileSystem fs = Assets.getInstance().getFs();
        Path file = new Path(path);
        if(fs.exists(file)){
            fs.delete(file, true);
            return true;
        }
        return false;
    }

    public static synchronized void deleteDirWithLog(JobConfiguration config,  String hdfsDir) throws SQLException, IOException, XMLStreamException {
        if(deleteFromHadoop(hdfsDir)){
            Log.write(config, "Delete "+ hdfsDir );
        }
    }

    public static synchronized boolean writeToHdfs(String pathStr, String str) throws XMLStreamException, IOException, SQLException {

        FileSystem fs = Assets.getInstance().getFs();
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

    public static synchronized String readFromHdfs(String pathStr) throws XMLStreamException, IOException, SQLException {
        FileSystem fs = Assets.getInstance().getFs();
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
