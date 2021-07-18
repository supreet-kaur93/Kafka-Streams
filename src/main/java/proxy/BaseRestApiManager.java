package proxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
public class BaseRestApiManager {

    private static final Logger log = LoggerFactory.getLogger(BaseRestApiManager.class);

    public <T> T get(String baseUrl, String resourceUrl, String query, HttpHeaders requestHeaders, Class<T> responseClassType) {
        ResponseEntity<T> responseEntity = null;
        String fullUrl = getFullUrl(baseUrl, resourceUrl, query);
        try {
            HttpEntity<Object> requestEntity = new HttpEntity<Object>(requestHeaders);
            RestTemplate restTemplate = new RestTemplate();
            responseEntity = restTemplate.exchange(fullUrl, HttpMethod.GET, requestEntity, responseClassType);
            if (responseEntity.getStatusCode() == HttpStatus.OK) {
                return responseEntity.getBody();
            }
        } catch (Exception e) {
            log.error("Error in RestApiManager:get : {} ; Exception : {}  , Full URL:: ", responseEntity, e, fullUrl);

        }
        return null;
    }

    private String getFullUrl(String baseUrl, String url, String query) {
        StringBuilder fullUrl = new StringBuilder();
        fullUrl.append(baseUrl);
        if (url != null) {
            fullUrl.append(url);
        }
        if (query != null && query.startsWith("?")) {
            query = query.substring(1);
        }
        query = (query == null) ? null : query.trim();
        query = (query == null || query.length() == 0) ? null : query;
        if (query != null) {
            fullUrl.append("?");
            fullUrl.append(query);
        }
        return fullUrl.toString();
    }
}