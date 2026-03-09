package ru.yandex.practicum.commerce.order.client;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import ru.yandex.practicum.commerce.order.dto.ProductDto;

@Component
@RequiredArgsConstructor
public class StoreClient {

    private static final String SERVICE_ID = "http://shopping-store";
    private final RestTemplate restTemplate;

    public ProductDto getProduct(Long productId) {
        return restTemplate.getForObject(SERVICE_ID + "/products/" + productId, ProductDto.class);
    }
}
