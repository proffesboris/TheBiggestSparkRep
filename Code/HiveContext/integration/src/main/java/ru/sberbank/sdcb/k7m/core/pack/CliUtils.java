package ru.sberbank.sdcb.k7m.core.pack;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Утилитарный класс взаимодействия с командной строкой
 */
public final class CliUtils {

    private CliUtils() {

    }

    /**
     * Вызов утилиты rsync
     * @param files список файлов
     * @param destinationFolder конечный каталог для перемещения
     */
    public static void rsync(List<File> files, String destinationFolder) {
        files.forEach(file -> {
            String filePath = file.getAbsolutePath();
            LogUtils.log("Execute: rsync --progress " + filePath + " " + destinationFolder);
            ProcessBuilder processBuilder = new ProcessBuilder()
                    .command("rsync", "--progress", filePath, destinationFolder)
                    .inheritIO()
                    .redirectErrorStream(true);
            try {
                Process process = processBuilder.start();
                if (process.waitFor() != 0) {
                    throw new RuntimeException("Ошибка выгрузки в смонтированный каталог!");
                }
            } catch (IOException e) {
                throw new IllegalStateException("Ошибка запуска rsync", e);
            } catch (InterruptedException e) {
                throw new IllegalStateException("Ошибка запуска rsync", e);
            }
        });

    }
}
