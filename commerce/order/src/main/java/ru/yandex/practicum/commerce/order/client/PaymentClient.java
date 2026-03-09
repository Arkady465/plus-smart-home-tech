package ru.yandex.practicum.commerce.order.client;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.math.BigDecimal;

@Component
@RequiredArgsConstructor
public class PaymentClient {

    private static final String SERVICE_ID = "http://payment";
    private final RestTemplate restTemplate;

    public void processPayment(Long orderId, BigDecimal amount) {
        restTemplate.postForObject(
                SERVICE_ID + "/",
                new PaymentRequest(orderId, amount),
                Void.class
        );
    }

    record PaymentRequest(Long orderId, BigDecimal amount) {}
}
