/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 *
 * Класс осуществяющий сгрузку данных из *.jxml файла конфигурации в класс JobConfiguration
 */

package ru.yakimov.config;

import jodd.util.MathUtil;
import ru.yakimov.BootProcessMain;
import ru.yakimov.LogDB.Log;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.EndElement;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.util.Iterator;

public class JobXmlLoader {

    private static final String HOST = "host";
    private static final String PORT = "port";

    private static final String USER = "user";
    private static final String PASSWORD = "password";
    private static final String SCHEMA = "schema";
    private static final String TABLE = "table";
    private static final String PRIMARY_KEY = "primaryKey";

    private static final String ROOT_JOB = "rootJob";
    private static final String JOB = "job";

    private static final String JOB_CLASS = "jobClass";
    private static final String STAGE = "stage";
    private static final String JOB_DIR_FROM = "jobDirFrom";
    private static final String JOB_DIR_TO = "jobDirTo";


    private static final String PARTITION = "partition";

    private static final String DB_CONF = "dbConf";

    /**
     * Метод сгрузки данных из *.jxml файла
     * @param file
     * @return
     */
    public static RootJobConfiguration readConfJob(File file){
        RootJobConfiguration resConfig = null;
        try{
            XMLInputFactory inputFactory = XMLInputFactory.newInstance();
            InputStream in = new FileInputStream(file);
            XMLEventReader eventReader = inputFactory.createXMLEventReader(in);
            RootJobConfiguration config = null;
            JobConfiguration jobConfig = null;
            while(eventReader.hasNext()){
                XMLEvent event = eventReader.nextEvent();
                if(event.isStartElement()){
                    StartElement startElementConf = event.asStartElement();
                    if (startElementConf.getName().getLocalPart().equals(ROOT_JOB)) {
                        config = new RootJobConfiguration(createJobNameFromPath(file));
                    }
                    if (event.isStartElement()) {
                        if (startElementConf.getName().getLocalPart().equals(JOB) && config != null) {
                            jobConfig = createJobConfig(config.getRootJobName(), startElementConf);
                        }
                    }
                    if(event.isStartElement()){
                        if (startElementConf.getName().getLocalPart().equals(DB_CONF) && jobConfig != null) {
                            jobConfig.setDbConfiguration(createDbConfig(startElementConf));
                        }
                    }
                }
                if(event.isEndElement()){
                    EndElement endElement = event.asEndElement();
                    if(endElement.getName().getLocalPart().equals(JOB) && jobConfig != null ){
                        config.addJobConfiguration(jobConfig);
                        jobConfig = null;
                    }
                    if(endElement.getName().getLocalPart().equals(ROOT_JOB)){
                        if(config != null && config.getRootJobName() != null) {
                            resConfig = config;
                            Log.writeRoot(BootProcessMain.MAIN_PROS, "Configuration have gotten for " + resConfig.getRootJobName());
                        }
                        break;
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            Log.writeSysException(BootProcessMain.MAIN_PROS, e);
        }
        return resConfig;
    }

    /**
     * Метод создания и наполнения данными JobConfiguration
     * @param rootJobName
     * @param startElementConf
     * @return
     */
    @SuppressWarnings("unchecked")
    private static JobConfiguration  createJobConfig(String rootJobName, StartElement startElementConf) {
        JobConfiguration jobConf = new JobConfiguration(rootJobName);
        Iterator<Attribute> attributes = startElementConf.getAttributes();
        while (attributes.hasNext()) {
            Attribute attribute = attributes.next();

            if (attribute.getName().toString().equals(JOB_CLASS)) {
                jobConf.setJobClass(attribute.getValue());
                jobConf.setJobName(createJobNameFromClass(attribute.getValue()));
            }
            if (attribute.getName().toString().equals(STAGE)) {
                jobConf.setStage(Integer.parseInt(attribute.getValue()));
            }
            if (attribute.getName().toString().equals(JOB_DIR_FROM)) {
                jobConf.setDirFrom(attribute.getValue());
            }
            if (attribute.getName().toString().equals(JOB_DIR_TO)) {
                jobConf.setDirTo(attribute.getValue());
            }
            if (attribute.getName().toString().equals(PARTITION)) {

                jobConf.setPartitions(attribute.getValue());
            }
        }
        return jobConf;
    }

    /**
     * Метод создания и наполнения данными DBConfiguration
     * @param startElementConf
     * @return
     */
    public static DBConfiguration createDbConfig(StartElement startElementConf) {
        DBConfiguration dbConfig = new DBConfiguration();
        setParamsTable(startElementConf, dbConfig);
        return dbConfig;
    }

    /**
     * Метод создания имени задания испотльзуя имя файла
     * @param file
     * @return
     */
    private static String createJobNameFromPath(File file){
        String fileName = file.getName().split("\\.")[0];
        return createNameWithData(fileName);
    }

    /**
     * Метод получения
     * @param ame
     * @return
     */
    public static String createNameWithData(String ame){
        String data = getFullTime();
        return ame+ "_" + data;
    }

    /**
     * Метод создания имени задания из используемого класса и даты и случайного числа
     * @param classPath
     * @return
     */
    private static String createJobNameFromClass(String classPath){
        String[] jobClassArr = classPath.split("\\.");
        String className = jobClassArr[jobClassArr.length-1];
        String data = getFullTime();
        return className + "_" + data + MathUtil.randomInt(0, 100000);
    }

    /**
     * Метод возвращает время представленное только цыфрами
     * @return
     */
    private static String getFullTime(){
        return getDataNumbersOnly(LocalDateTime.now().toString());
    }

    /**
     * Метод возврящающий только числа из строки
     * @param string
     * @return
     */
    public static String getDataNumbersOnly(String string){
        return string.replaceAll("[^0-9?!]", "");
    }


    public static void setParamsTable(StartElement startElementConf, DBConfiguration dbConfig) {
        Iterator<Attribute> attributes = startElementConf.getAttributes();
        while (attributes.hasNext()) {
            Attribute attribute = attributes.next();
            if (attribute.getName().toString().equals(HOST)) {
                dbConfig.setHost(attribute.getValue());
            }
            if (attribute.getName().toString().equals(PORT)) {
                dbConfig.setPort(attribute.getValue());
            }
            if (attribute.getName().toString().equals(USER)) {
                dbConfig.setUser(attribute.getValue());
            }
            if (attribute.getName().toString().equals(PASSWORD)) {
                dbConfig.setPassword(attribute.getValue());
            }
            if (attribute.getName().toString().equals(SCHEMA)) {
                dbConfig.setSchema(attribute.getValue());
            }
            if (attribute.getName().toString().equals(TABLE)) {
                dbConfig.setTable(attribute.getValue());
            }
            if (attribute.getName().toString().equals(PRIMARY_KEY)) {
                dbConfig.setPrimaryKey(attribute.getValue());
            }
        }
    }

}
