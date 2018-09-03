package ru.sberbank.sdcb.k7m.core.pack.rest;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.springframework.http.*;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;

public class CtlCheckMain {

    private static final String URI_PATTERN = "http://septu4.df.sbrf.ru:8079/v1/api/statval/%s/12";
    private static final String DATE_COLUMN = "upload_date";
    private static final String ID_COLUMN = "id";
    private static final String FORMAT_PATTERN = "dd.MM.yyyy-HH:mm:ss";

    public static void main(String[] args) throws IOException {

        List<CSVRecord> records;
        List<String> columns;
        Path path = Paths.get(args[0]);
        String fileName = path.getFileName().toString();
        try (Reader reader = Files.newBufferedReader(path);
             CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                     .withDelimiter(';')
                     .withFirstRecordAsHeader()
                     .withIgnoreHeaderCase()
                     .withTrim())) {

            records = csvParser.getRecords();
            columns = new ArrayList<>(csvParser.getHeaderMap().keySet());
        }

        RestTemplate restTemplate = buildRestTemplate();
        HttpEntity<?> httpEntity = buildHttpEntity();
        Map<CSVRecord, String> resultOutput = new HashMap<>();

        records.forEach(record -> {
            String id = record.get(ID_COLUMN);
            String uri = String.format(URI_PATTERN, id);
            String uploadDate;
            try {
                ResponseEntity<Statval[]> response = restTemplate.exchange(uri, HttpMethod.GET, httpEntity, Statval[].class);
                List<Statval> result = Arrays.asList(response.getBody());
                uploadDate = getMaxUploadDate(result);
            } catch (HttpClientErrorException e) {
                uploadDate = "ERROR status code= " + e.getStatusCode();
            }

            resultOutput.put(record, uploadDate);

        });

        List<String> newColumnsList = new ArrayList<>(columns);
        newColumnsList.add(DATE_COLUMN);
        String[] newColumns = newColumnsList.toArray(new String[0]);
        Path outputFile = Paths.get(path.getParent().toString(), "out_" + fileName);
        try (BufferedWriter writer = Files.newBufferedWriter(outputFile);
             CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT
                     .withHeader(newColumns)
                     .withDelimiter(';'))) {

            resultOutput.forEach((record, uploadDate) -> {
                List<String> values = new ArrayList<>();
                columns.forEach(column -> {
                    values.add(record.get(column));
                });
                values.add(uploadDate);
                try {
                    csvPrinter.printRecord(values);
                } catch (IOException e) {
                    throw new RuntimeException("Ошибка записи", e);
                }
            });
            csvPrinter.flush();
        }
    }

    private static String getMaxUploadDate(List<Statval> result) {
        result.sort(Comparator.comparing(Statval::getValue).reversed());
        SimpleDateFormat sdf = new SimpleDateFormat(FORMAT_PATTERN);
        return sdf.format(result.get(0).getValue());
    }

    private static HttpEntity<?> buildHttpEntity() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setAccept(Collections.singletonList(MediaType.ALL));
        return new HttpEntity<>(headers);
    }

    private static RestTemplate buildRestTemplate() {
        MappingJackson2HttpMessageConverter converter = new MappingJackson2HttpMessageConverter();
        converter.setSupportedMediaTypes(Arrays.asList(MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON));
        RestTemplate restTemplate = new RestTemplate();
        restTemplate.getMessageConverters().add(converter);
        return restTemplate;
    }

}
