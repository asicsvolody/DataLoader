/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 */

package ru.yakimov.VisualFXML;

import javafx.fxml.FXML;
import javafx.scene.control.Button;
import javafx.scene.control.PasswordField;
import javafx.scene.control.TextField;
import ru.yakimov.BootProcessMain;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.Map;

//import static ru.yakimov.Assets.CONF_FILE_PATH;


public class ControllerFXML {

    @FXML
    private TextField mysqlDomain;

    @FXML
    private TextField mysqlPort;

    @FXML
    private TextField mysqlUser;

    @FXML
    private PasswordField mysqlPass;

    @FXML
    private TextField mysqlDatabase;

    @FXML
    private TextField mysqlTable;

    @FXML
    private TextField hdfsDomain;

    @FXML
    private TextField hdfsPort;

    @FXML
    private TextField hdfsDir;

    @FXML
    private Button startBtn;

    private Map confData;

    @FXML
    private void start(){
        String [] args = getConfArgs();
//        if(new ConfDataInspector(args).isDataCorrect()){
////            writeResToFile(args, CONF_FILE_PATH);
//        }
        BootProcessMain.main(new String[0]);
    }

    private String[] getConfArgs(){
        return new String[]{
                mysqlDomain.getText(),
                mysqlPort.getText(),
                mysqlUser.getText(),
                mysqlPass.getText(),
                mysqlDatabase.getText(),
                mysqlTable.getText(),
                hdfsDomain.getText(),
                hdfsPort.getText(),
                hdfsDir.getText()
        };
    }

    private static void writeResToFile(String[]args,  String confFilePath){
        try(PrintWriter out = new PrintWriter(new FileOutputStream(confFilePath))) {
            for (String arg : args) {
                out.println(arg);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
}
