package ru.yandex.practicum.commerce.payment.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.commerce.payment.model.Payment;
import ru.yandex.practicum.commerce.payment.repository.PaymentRepository;

import java.math.BigDecimal;
import java.time.Instant;

@RestController
@RequestMapping
@RequiredArgsConstructor
public class PaymentController {

    private final PaymentRepository paymentRepository;

    @PostMapping
    public Payment process(@RequestBody ProcessPaymentRequest request) {
        Payment payment = Payment.builder()
                .orderId(request.orderId())
                .amount(request.amount())
                .status(Payment.PaymentStatus.COMPLETED)
                .paidAt(Instant.now())
                .build();
        return paymentRepository.save(payment);
    }

    @GetMapping("/order/{orderId}")
    public ResponseEntity<Payment> getByOrderId(@PathVariable Long orderId) {
        return paymentRepository.findByOrderId(orderId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    public record ProcessPaymentRequest(Long orderId, BigDecimal amount) {}
}
