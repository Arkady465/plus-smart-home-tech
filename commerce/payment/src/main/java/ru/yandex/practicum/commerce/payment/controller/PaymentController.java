package ru.yandex.practicum.commerce.payment.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.commerce.api.PaymentClient;
import ru.yandex.practicum.commerce.dto.*;
import ru.yandex.practicum.commerce.payment.service.PaymentService;

@RestController
@RequiredArgsConstructor
public class PaymentController implements PaymentClient {

    private final PaymentService paymentService;

    @Override
    @PostMapping("/api/v1/payment/productCost")
    public ProductCostResponseDto productCost(@RequestBody ProductCostRequestDto request) {
        return paymentService.productCost(request);
    }

    @Override
    @PostMapping("/api/v1/payment/totalCost")
    public TotalCostResponseDto getTotalCost(@RequestBody TotalCostRequestDto request) {
        return paymentService.totalCost(request);
    }

    @Override
    @PostMapping("/api/v1/payment")
    public PaymentDto createPayment(@RequestBody PaymentDto request) {
        return paymentService.createPayment(request);
    }

    @Override
    @PostMapping("/api/v1/payment/{paymentId}/success")
    public PaymentDto success(@PathVariable Long paymentId) {
        return paymentService.markSuccess(paymentId);
    }

    @Override
    @PostMapping("/api/v1/payment/{paymentId}/failed")
    public PaymentDto failed(@PathVariable Long paymentId) {
        return paymentService.markFailed(paymentId);
    }
}

