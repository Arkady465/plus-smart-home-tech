package ru.yandex.practicum.commerce.order.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.commerce.dto.*;
import ru.yandex.practicum.commerce.order.entity.OrderEntity;
import ru.yandex.practicum.commerce.order.entity.OrderItemEntity;
import ru.yandex.practicum.commerce.order.repository.OrderRepository;

import java.util.List;

@Service
@RequiredArgsConstructor
public class OrderService {

    private final OrderRepository orderRepository;

    @Transactional
    public OrderDto createOrder(OrderCreateRequestDto request) {
        OrderEntity order = OrderEntity.builder()
                .username(request.getUsername())
                .cartId(request.getCartId())
                .status(OrderStatus.NEW)
                .build();

        if (request.getItems() != null) {
            for (OrderItemDto item : request.getItems()) {
                if (item == null || item.getProductId() == null || item.getProductId().isBlank()) continue;
                OrderItemEntity entity = OrderItemEntity.builder()
                        .order(order)
                        .productId(item.getProductId())
                        .quantity(Math.max(0, item.getQuantity()))
                        .build();
                order.getItems().add(entity);
            }
        }

        order = orderRepository.save(order);
        return toDto(order);
    }

    public List<OrderDto> getOrders(String username) {
        return orderRepository.findAllByUsernameOrderByIdDesc(username).stream().map(this::toDto).toList();
    }

    @Transactional
    public OrderDto updateStatus(Long orderId, OrderStatus status) {
        OrderEntity order = orderRepository.findById(orderId)
                .orElseThrow(() -> new RuntimeException("Order not found: " + orderId));
        order.setStatus(status);
        order = orderRepository.save(order);
        return toDto(order);
    }

    private OrderDto toDto(OrderEntity order) {
        List<OrderItemDto> items = order.getItems() == null ? List.of() : order.getItems().stream()
                .map(i -> OrderItemDto.builder().productId(i.getProductId()).quantity(i.getQuantity()).build())
                .toList();
        return OrderDto.builder()
                .id(order.getId())
                .username(order.getUsername())
                .status(order.getStatus())
                .cartId(order.getCartId())
                .paymentId(order.getPaymentId())
                .deliveryId(order.getDeliveryId())
                .items(items)
                .totalCost(order.getTotalCost())
                .productsCost(order.getProductsCost())
                .deliveryCost(order.getDeliveryCost())
                .weight(order.getWeight())
                .volume(order.getVolume())
                .fragile(order.isFragile())
                .build();
    }
}

