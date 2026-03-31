package ru.yandex.practicum.commerce.payment.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.commerce.api.OrderClient;
import ru.yandex.practicum.commerce.api.ShoppingStoreClient;
import ru.yandex.practicum.commerce.dto.*;
import ru.yandex.practicum.commerce.payment.entity.PaymentEntity;
import ru.yandex.practicum.commerce.payment.repository.PaymentRepository;

@Service
@RequiredArgsConstructor
public class PaymentService {

    private final PaymentRepository paymentRepository;
    private final ShoppingStoreClient shoppingStoreClient;
    private final OrderClient orderClient;

    public ProductCostResponseDto productCost(ProductCostRequestDto request) {
        double sum = 0.0;
        if (request.getItems() != null) {
            for (OrderItemDto item : request.getItems()) {
                if (item == null || item.getProductId() == null) continue;
                Long id = parseLongOrNull(item.getProductId());
                if (id == null) continue;
                ProductDto product = shoppingStoreClient.getProduct(id);
                Double price = product.getPrice();
                if (price == null) price = 0.0;
                sum += price * Math.max(0, item.getQuantity());
            }
        }
        return ProductCostResponseDto.builder()
                .orderId(request.getOrderId())
                .productsCost(sum)
                .build();
    }

    public TotalCostResponseDto totalCost(TotalCostRequestDto request) {
        double productsCost = request.getProductsCost() != null ? request.getProductsCost() : 0.0;
        double deliveryCost = request.getDeliveryCost() != null ? request.getDeliveryCost() : 0.0;
        double tax = productsCost * 0.10;
        double total = productsCost + tax + deliveryCost;
        return TotalCostResponseDto.builder()
                .orderId(request.getOrderId())
                .tax(tax)
                .totalCost(total)
                .build();
    }

    @Transactional
    public PaymentDto createPayment(PaymentDto request) {
        PaymentEntity entity = PaymentEntity.builder()
                .orderId(request.getOrderId())
                .productsCost(request.getProductsCost())
                .deliveryCost(request.getDeliveryCost())
                .totalCost(request.getTotalCost())
                .status(request.getStatus() != null ? request.getStatus() : PaymentStatus.PENDING)
                .build();
        entity = paymentRepository.save(entity);
        return toDto(entity);
    }

    @Transactional
    public PaymentDto markSuccess(Long paymentId) {
        PaymentEntity entity = paymentRepository.findById(paymentId)
                .orElseThrow(() -> new RuntimeException("Payment not found: " + paymentId));
        entity.setStatus(PaymentStatus.SUCCESS);
        entity = paymentRepository.save(entity);
        orderClient.paymentSuccess(entity.getOrderId());
        return toDto(entity);
    }

    @Transactional
    public PaymentDto markFailed(Long paymentId) {
        PaymentEntity entity = paymentRepository.findById(paymentId)
                .orElseThrow(() -> new RuntimeException("Payment not found: " + paymentId));
        entity.setStatus(PaymentStatus.FAILED);
        entity = paymentRepository.save(entity);
        orderClient.paymentFailed(entity.getOrderId());
        return toDto(entity);
    }

    private static Long parseLongOrNull(String s) {
        if (s == null) return null;
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    private PaymentDto toDto(PaymentEntity entity) {
        return PaymentDto.builder()
                .id(entity.getId())
                .orderId(entity.getOrderId())
                .productsCost(entity.getProductsCost())
                .deliveryCost(entity.getDeliveryCost())
                .totalCost(entity.getTotalCost())
                .status(entity.getStatus())
                .build();
    }
}

