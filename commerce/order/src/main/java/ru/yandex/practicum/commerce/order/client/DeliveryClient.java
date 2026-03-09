package ru.yandex.practicum.commerce.order.client;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
@RequiredArgsConstructor
public class DeliveryClient {

    private static final String SERVICE_ID = "http://delivery";
    private final RestTemplate restTemplate;

    public void createDelivery(Long orderId, String address) {
        restTemplate.postForObject(
                SERVICE_ID + "/",
                new CreateDeliveryRequest(orderId, address),
                Void.class
        );
    }

    public record CreateDeliveryRequest(Long orderId, String address) {}
}
