/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 * Класс осуществяющий сгрузку данных из *.xml файла конфигурации в класс AppConfiguration
 */

package ru.yakimov.config;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.EndElement;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Iterator;

public class AppXmlLoader {
    private static final String CONFIGURATION = "configuration";

    private static final String HOST = "host";
    private static final String PORT = "port";
    private static final String TEMP_DIR = "tempDir";
    private static final String JOBS_DIR = "jobsDir";
    private static final String LOGS_DIR = "logsDir";

    private static final String WORK_SCHEMA = "workSchema";

    private static final String LOG_TABLE = "logTable";
    private static final String STRUCT_TABLE = "structTable";

    private static final String USER = "user";
    private static final String PASSWORD = "password";
    private static final String SCHEMA = "schema";
    private static final String TABLE = "table";
    private static final String PRIMARY_KEY = "primaryKey";


    /**
     * Метод сгрузки данных из конфигурационного файла
     *
     * @param configFile
     * @return
     * @throws FileNotFoundException
     * @throws XMLStreamException
     */
    public static AppConfiguration readConfigApp(String configFile) throws FileNotFoundException, XMLStreamException {
        AppConfiguration resConfig = null;
        try {

            XMLInputFactory inputFactory = XMLInputFactory.newInstance();
            InputStream in = new FileInputStream(configFile);
            XMLEventReader eventReader = inputFactory.createXMLEventReader(in);
            AppConfiguration config = new AppConfiguration();
            while (eventReader.hasNext()) {
                XMLEvent event = eventReader.nextEvent();
                if (event.isStartElement()) {
                    StartElement startElementConf = event.asStartElement();
                    if (startElementConf.getName().getLocalPart().equals(CONFIGURATION)) {
                        Iterator<Attribute> attributes = startElementConf.getAttributes();
                        while (attributes.hasNext()) {
                            Attribute attribute = attributes.next();

                            if (attribute.getName().toString().equals(HOST)) {
                                config.setHdfsHost(attribute.getValue());
                            }
                            if (attribute.getName().toString().equals(PORT)) {
                                config.setHdfsPort(attribute.getValue());
                            }
                            if (attribute.getName().toString().equals(TEMP_DIR)) {
                                config.setTmpDir(attribute.getValue());
                            }
                            if (attribute.getName().toString().equals(JOBS_DIR)) {
                                config.setJobsDir(attribute.getValue());
                            }
                            if (attribute.getName().toString().equals(LOGS_DIR)) {
                                config.setLogsDir(attribute.getValue());
                            }
                            if (attribute.getName().toString().equals(LOG_TABLE)) {
                                config.setLogTable(attribute.getValue());
                            }if (attribute.getName().toString().equals(STRUCT_TABLE)) {
                                config.setStructTable(attribute.getValue());
                            }
                        }
                    }
                    if (event.isStartElement()) {
                        if (event.asStartElement().getName().getLocalPart().equals(HOST)) {
                            event = eventReader.nextEvent();
                            config.setHdfsHost(event.asCharacters().getData());
                            continue;
                        }
                    }
                    if (event.isStartElement()) {
                        if (event.asStartElement().getName().getLocalPart().equals(PORT)) {
                            event = eventReader.nextEvent();
                            config.setHdfsPort(event.asCharacters().getData());
                            continue;
                        }
                    }
                    if (event.isStartElement()) {
                        if (event.asStartElement().getName().getLocalPart().equals(TEMP_DIR)) {
                            event = eventReader.nextEvent();
                            config.setTmpDir(event.asCharacters().getData());
                            continue;
                        }
                    }
                    if (event.isStartElement()) {
                        if (event.asStartElement().getName().getLocalPart().equals(JOBS_DIR)) {
                            event = eventReader.nextEvent();
                            config.setJobsDir(event.asCharacters().getData());
                            continue;
                        }
                    }
                    if (event.isStartElement()) {
                        if (event.asStartElement().getName().getLocalPart().equals(LOGS_DIR)) {
                            event = eventReader.nextEvent();
                            config.setLogsDir(event.asCharacters().getData());
                            continue;
                        }
                    }
                    if (event.isStartElement()) {
                        if (startElementConf.getName().getLocalPart().equals(WORK_SCHEMA)) {
                            DBConfiguration workSchema = new DBConfiguration();
                            setParamsTable(startElementConf, workSchema);

                            config.setWorkSchema(workSchema);
                        }
                    }

                    if (event.isStartElement()) {
                        if (event.asStartElement().getName().getLocalPart().equals(LOG_TABLE)) {
                            event = eventReader.nextEvent();
                            config.setLogTable(event.asCharacters().getData());
                            continue;
                        }
                    }if (event.isStartElement()) {
                        if (event.asStartElement().getName().getLocalPart().equals(STRUCT_TABLE)) {
                            event = eventReader.nextEvent();
                            config.setStructTable(event.asCharacters().getData());
                            continue;
                        }
                    }


                }
                if(event.isEndElement()){
                    EndElement endElement = event.asEndElement();
                    if(endElement.getName().getLocalPart().equals(CONFIGURATION)){
                        resConfig = config;
                        break;
                    }
                }

            }

        }catch (FileNotFoundException e) {
            throw new FileNotFoundException("Not found conf file");
        }
        return resConfig;
    }

    /**
     * Отдельный метод создания и наполнения данными DBConfiguration при стении конфигурациорнного файла
     * @param startElementConf
     * @param dbConfig
     */
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
