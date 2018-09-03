package ru.sberbank.sdcb.k7m.core.pack;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Утилитарный класс для работы с файлами
 */
public final class FileUtils {

    private FileUtils() {

    }

    /**
     * Полуучение файлов из каталога с логгированием в консоль
     * @param path папка
     * @return список файлов
     */
    public static List<File> obtainFiles(String path) {
        try (Stream<Path> stream = Files.walk(Paths.get(path))) {
            List<File> files = stream
                    .filter(Files::isRegularFile)
                    .map(Path::toFile)
                    .collect(Collectors.toList());
            if (files.isEmpty()) {
                throw new IllegalStateException("Нет файлов для выгрузки!");
            }
            LogUtils.log("Files", files
                    .stream()
                    .map(File::getName).collect(Collectors.joining("\n")));
            return files;
        } catch (IOException e) {
            throw new RuntimeException("Ошибка обработки каталога: " + path, e);
        }
    }
}
