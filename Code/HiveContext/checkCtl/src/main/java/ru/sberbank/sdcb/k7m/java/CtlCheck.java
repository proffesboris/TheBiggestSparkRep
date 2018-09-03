package ru.sberbank.sdcb.k7m.java;

import org.springframework.http.*;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.web.client.AsyncRestOperations;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class CtlCheck implements Serializable {

    private static final String URI_PATTERN = "http://septu4.df.sbrf.ru:8079/v1/api/statval/%s/12";
    private static final String DATE_COLUMN = "upload_date";
    private static final String ID_COLUMN = "id";
    private static final String FORMAT_PATTERN = "dd.MM.yyyy-HH:mm:ss";

    private transient RestTemplate restTemplate;
    private transient HttpEntity<?> httpEntity;

    public CtlCheck() {
        restTemplate = buildRestTemplate();
        httpEntity = buildHttpEntity();
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

    public String check(String id) {
        String uri = String.format(URI_PATTERN, id);
        try {
            ResponseEntity<Statval[]> response = restTemplate().exchange(uri, HttpMethod.GET, httpEntity(), Statval[].class);
            List<Statval> result = Arrays.asList(response.getBody());
            return getMaxUploadDate(result);
        } catch (HttpClientErrorException e) {
            return "ERROR status code= " + e.getStatusCode();
        }

    }

    private HttpEntity<?> httpEntity() {
        if (httpEntity == null) {
            httpEntity = buildHttpEntity();
        }
        return httpEntity;
    }

    private RestTemplate restTemplate() {
        if (restTemplate == null) {
            restTemplate = buildRestTemplate();
        }
        return restTemplate;
    }
}
