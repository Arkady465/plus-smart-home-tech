package ru.yandex.practicum.commerce.order.client;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import ru.yandex.practicum.commerce.order.dto.CartDto;

@Component
@RequiredArgsConstructor
public class CartClient {

    private static final String SERVICE_ID = "http://shopping-cart";
    private final RestTemplate restTemplate;

    public CartDto getCart(Long userId) {
        return restTemplate.getForObject(SERVICE_ID + "/" + userId, CartDto.class);
    }
}
