package ru.yandex.practicum.commerce.api;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.commerce.dto.*;

@FeignClient(name = "payment")
public interface PaymentClient {

    @PostMapping("/api/v1/payment/productCost")
    ProductCostResponseDto productCost(@RequestBody ProductCostRequestDto request);

    @PostMapping("/api/v1/payment/totalCost")
    TotalCostResponseDto getTotalCost(@RequestBody TotalCostRequestDto request);

    @PostMapping("/api/v1/payment")
    PaymentDto createPayment(@RequestBody PaymentDto request);

    @PostMapping("/api/v1/payment/{paymentId}/success")
    PaymentDto success(@PathVariable Long paymentId);

    @PostMapping("/api/v1/payment/{paymentId}/failed")
    PaymentDto failed(@PathVariable Long paymentId);
}

