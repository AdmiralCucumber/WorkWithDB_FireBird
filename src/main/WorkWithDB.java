package main;

import db.AccessToFB;
import db.RecordReport;
import db.SQLQueryFromFB;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.logging.Logger;

import static utils.ConstantForAll.*;

class WorkWithDB {
    private Logger logger;
    private AccessToFB accessToFB;

    WorkWithDB(Logger logger) {
        this.logger = logger;
        initAccessToFB();

        SQLQueryFromFB sqlQueryFromFB = null;
        try {
            sqlQueryFromFB = new SQLQueryFromFB(logger, accessToFB.getConnection());
        } catch (SQLException e) {
            logger.info("ошибка SQLQueryFromFB: " + e.getMessage());
        }
        String strTableName = "TableForTesting";
        try {
            //проверяем, есть ли такая таблица
            if (!sqlQueryFromFB.isTableExists(strTableName)) {
                //если её нет - создаём
                if (sqlQueryFromFB.createTable(strTableName))
                    logger.info("таблица " + strTableName + " создана");
            } else {
                logger.info("таблица " + strTableName + " найдена");
                logger.info("");
                logger.info("--- удаляем все данные из таблицы");
                sqlQueryFromFB.deleteAllDataFromTable(strTableName);
                logger.info("");
                logger.info("--- заносим новые данные в таблицу");
                sqlQueryFromFB.insertDataToTable(strTableName, 1, "Иконка Java", IMG_JAVA_PNG);
                sqlQueryFromFB.insertDataToTable(strTableName, 2, "Иконка FireBird", IMG_FIREBIRD_PNG);
                sqlQueryFromFB.insertDataToTable(strTableName, 3, "Иконка WordPress", IMG_WORDPRESS_PNG);
                logger.info("");
                logger.info("--- считываем все данные из таблицы");
                ArrayList<RecordReport> listRecord = sqlQueryFromFB.getReport_AllFieldsFromTable(strTableName);
                logger.info("--- выводим полученные данные в логгер");
                if (listRecord.size() != 0) {
                    for (RecordReport itemRecord : listRecord) {
                        logger.info(itemRecord.toString());
                    }
                }
                logger.info("");
                logger.info("--- добавляем в БД файлы изображений");
                sqlQueryFromFB.updateDataFileFromResourceToTable(strTableName, 1, "Img", IMG_JAVA_PNG_INRES);
                sqlQueryFromFB.updateDataFileFromResourceToTable(strTableName, 2, "Img", IMG_FIREBIRD_PNG_INRES);
                sqlQueryFromFB.updateDataFileFromResourceToTable(strTableName, 3, "Img", IMG_WORDPRESS_PNG_INRES);
                logger.info("");
                logger.info("--- извлекаем из БД файлы изображений и сохраняем их в корневой папке");
                sqlQueryFromFB.getFileFromTableAndSaveIt(strTableName, 1, "Img", IMG_JAVA_PNG);
                sqlQueryFromFB.getFileFromTableAndSaveIt(strTableName, 2, "Img", IMG_FIREBIRD_PNG);
                sqlQueryFromFB.getFileFromTableAndSaveIt(strTableName, 3, "Img", IMG_WORDPRESS_PNG);
            }
        } finally {
            closeAccessToFB();
        }
    }

    private void initAccessToFB() {
        accessToFB = new AccessToFB(logger);
        if (accessToFB.getAccess()) {
            logger.info("доступ к БД получен");
        } else {
            logger.info("ошибка доступа к БД");
            System.exit(1);
        }
    }

    private void closeAccessToFB() {
        accessToFB.closeAccess();
        logger.info("доступ к БД отключен");
    }
}
