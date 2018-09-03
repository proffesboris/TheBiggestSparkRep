package ru.sberbank.sdcb.k7m.core.pack;

import com.sbt.tfs.integration.SendFileInfoNf;
import com.sbt.tfs.integration.SendFileStatusNf;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import java.io.StringReader;
import java.io.StringWriter;

/**
 * Утилитарный класс (де)сериализации из/в XML
 */
public final class JaxbUtils {

    private static final JAXBContext JAXB_CONTEXT;

    static {
        try {
            JAXB_CONTEXT = JAXBContext.newInstance(SendFileInfoNf.class, SendFileStatusNf.class);
        } catch (JAXBException e) {
            throw new IllegalStateException("Ошибка инициализации JAXB!", e);
        }
    }

    private JaxbUtils() {
    }

    /**
     * Десериализация из XML
     * @param message строка с сообщением
     * @return объект-сообщение
     */
    @SuppressWarnings("unchecked")
    public static <T> T unmarshal(String message) {
        try {
            Unmarshaller unmarshaller = JAXB_CONTEXT.createUnmarshaller();
            StringReader reader = new StringReader(message);
            return (T) unmarshaller.unmarshal(reader);
        } catch (JAXBException e) {
            throw new RuntimeException("Ошибка десериализации обекта", e);
        }
    }

    /**
     * Сериализация в XML
     * @param message объект-сообщение
     * @return строка с сообщением
     */
    public static <T> String marshal(T message) {
        try {
            Marshaller marshaller = JAXB_CONTEXT.createMarshaller();
            StringWriter stringWriter = new StringWriter();
            marshaller.marshal(message, stringWriter);
            return stringWriter.toString();
        } catch (JAXBException e) {
            throw new RuntimeException("Ошибка сериализации в строку", e);
        }
    }
}
