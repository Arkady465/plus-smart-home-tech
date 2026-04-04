package ru.yandex.practicum.commerce.payment.service;

import jakarta.persistence.EntityNotFoundException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.commerce.api.OrderClient;
import ru.yandex.practicum.commerce.api.ShoppingStoreClient;
import ru.yandex.practicum.commerce.dto.*;
import ru.yandex.practicum.commerce.payment.entity.PaymentEntity;
import ru.yandex.practicum.commerce.payment.repository.PaymentRepository;

@Slf4j
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
                if (item == null) {
                    log.warn("productCost: null order item, orderId={}", request.getOrderId());
                    throw new IllegalArgumentException("Order item must not be null");
                }
                String rawId = item.getProductId();
                if (rawId == null || rawId.isBlank()) {
                    log.warn("productCost: missing productId, orderId={}", request.getOrderId());
                    throw new IllegalArgumentException("Invalid productId");
                }
                long id = parseProductIdAsLong(rawId);
                ProductDto product;
                try {
                    product = shoppingStoreClient.getProduct(id);
                } catch (RuntimeException ex) {
                    log.error("productCost: failed to load productId={}, orderId={}", id, request.getOrderId(), ex);
                    throw ex;
                }
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
        log.info("Payment created id={} orderId={} status={}", entity.getId(), entity.getOrderId(), entity.getStatus());
        return toDto(entity);
    }

    @Transactional
    public PaymentDto markSuccess(Long paymentId) {
        PaymentEntity entity = paymentRepository.findById(paymentId)
                .orElseThrow(() -> new EntityNotFoundException("Payment not found: " + paymentId));
        PaymentStatus previous = entity.getStatus();
        entity.setStatus(PaymentStatus.SUCCESS);
        entity = paymentRepository.save(entity);
        log.info("Payment id={} orderId={} status transition {} -> {}", paymentId, entity.getOrderId(), previous, PaymentStatus.SUCCESS);
        orderClient.paymentSuccess(entity.getOrderId());
        return toDto(entity);
    }

    @Transactional
    public PaymentDto markFailed(Long paymentId) {
        PaymentEntity entity = paymentRepository.findById(paymentId)
                .orElseThrow(() -> new EntityNotFoundException("Payment not found: " + paymentId));
        PaymentStatus previous = entity.getStatus();
        entity.setStatus(PaymentStatus.FAILED);
        entity = paymentRepository.save(entity);
        log.info("Payment id={} orderId={} status transition {} -> {}", paymentId, entity.getOrderId(), previous, PaymentStatus.FAILED);
        orderClient.paymentFailed(entity.getOrderId());
        return toDto(entity);
    }

    private static long parseProductIdAsLong(String productId) {
        try {
            return Long.parseLong(productId.trim());
        } catch (NumberFormatException e) {
            log.warn("Invalid productId (not a number): {}", productId);
            throw new IllegalArgumentException("Invalid productId: " + productId);
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
