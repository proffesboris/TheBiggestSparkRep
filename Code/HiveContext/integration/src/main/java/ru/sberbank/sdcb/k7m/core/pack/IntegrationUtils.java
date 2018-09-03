package ru.sberbank.sdcb.k7m.core.pack;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.zip.CRC32;

/**
 * Утилитарный класс
 */
public final class IntegrationUtils {

    private IntegrationUtils() {
    }

    /**
     * Получение текущего времени для выгрузки
     *
     * @return текущее время для выгрузки
     */
    public static XMLGregorianCalendar currentTimestamp() {
        DatatypeFactory datatypeFactory;
        try {
            datatypeFactory = DatatypeFactory.newInstance();
        } catch (DatatypeConfigurationException e) {
            throw new IllegalStateException("Невозможно создать фабрику!", e);
        }
        GregorianCalendar gregorianCalendar = new GregorianCalendar();
        gregorianCalendar.setTime(new Date());
        return datatypeFactory.newXMLGregorianCalendar(gregorianCalendar);
    }

    /**
     * Подсчет контролько суммы файла по алгоритму CRC32
     *
     * @param file файл
     * @return контрольная сумма
     */
    public static long crc(File file) {
        try (FileInputStream in = new FileInputStream(file)) {
            CRC32 crcMaker = new CRC32();
            byte[] buffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = in.read(buffer)) != -1) {
                crcMaker.update(buffer, 0, bytesRead);
            }
            return crcMaker.getValue();
        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException("Не найден файл " + file.getName(), e);
        } catch (IOException e) {
            throw new IllegalStateException("Ошибка обработки файла " + file.getName(), e);
        }
    }
}
