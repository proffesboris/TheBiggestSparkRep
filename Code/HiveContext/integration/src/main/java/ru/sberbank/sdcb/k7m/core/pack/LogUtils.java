package ru.sberbank.sdcb.k7m.core.pack;

/**
 * Утилитарный класс логгирования в консоль(stdout)
 */
public final class LogUtils {

    private static final String TITLE_DELIMITER = "------------------------------------------------";

    private LogUtils() {

    }

    public static void logTitle(String title) {
        System.out.println(TITLE_DELIMITER + "\n" + title.toUpperCase() + "\n" + TITLE_DELIMITER);
    }

    public static void log(String message) {
        System.out.println(message);
    }

    public static void log(String title, String message) {
        logTitle(title);
        log(message);
    }
}
