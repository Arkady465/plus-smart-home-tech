package ru.yandex.practicum.commerce.order.client;

import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.Map;

@Component
@RequiredArgsConstructor
public class WarehouseClient {

    private static final String SERVICE_ID = "http://warehouse";
    private final RestTemplate restTemplate;

    public void reserve(Long productId, int quantity) {
        restTemplate.postForObject(
                SERVICE_ID + "/" + productId + "/reserve",
                Map.of("quantity", quantity),
                Void.class
        );
    }
}
