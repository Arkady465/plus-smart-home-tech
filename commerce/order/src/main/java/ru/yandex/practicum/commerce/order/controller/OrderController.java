package ru.yandex.practicum.commerce.order.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.commerce.api.OrderClient;
import ru.yandex.practicum.commerce.dto.OrderCreateRequestDto;
import ru.yandex.practicum.commerce.dto.OrderDto;
import ru.yandex.practicum.commerce.dto.OrderStatus;
import ru.yandex.practicum.commerce.order.service.OrderService;

import java.util.List;

@RestController
@RequiredArgsConstructor
public class OrderController implements OrderClient {

    private final OrderService orderService;

    @Override
    @PostMapping("/api/v1/order")
    public OrderDto createOrder(@RequestBody OrderCreateRequestDto request) {
        return orderService.createOrder(request);
    }

    @Override
    @GetMapping("/api/v1/order")
    public List<OrderDto> getOrders(@RequestParam String username) {
        return orderService.getOrders(username);
    }

    @Override
    @PostMapping("/api/v1/order/{orderId}/payment/success")
    public OrderDto paymentSuccess(@PathVariable Long orderId) {
        return orderService.updateStatus(orderId, OrderStatus.PAID);
    }

    @Override
    @PostMapping("/api/v1/order/{orderId}/payment/failed")
    public OrderDto paymentFailed(@PathVariable Long orderId) {
        return orderService.updateStatus(orderId, OrderStatus.PAYMENT_FAILED);
    }

    @Override
    @PostMapping("/api/v1/order/{orderId}/delivery/success")
    public OrderDto deliverySuccess(@PathVariable Long orderId) {
        return orderService.updateStatus(orderId, OrderStatus.DELIVERED);
    }

    @Override
    @PostMapping("/api/v1/order/{orderId}/delivery/failed")
    public OrderDto deliveryFailed(@PathVariable Long orderId) {
        return orderService.updateStatus(orderId, OrderStatus.DELIVERY_FAILED);
    }

    @Override
    @PostMapping("/api/v1/order/{orderId}/assembly/failed")
    public OrderDto assemblyFailed(@PathVariable Long orderId) {
        return orderService.updateStatus(orderId, OrderStatus.ASSEMBLY_FAILED);
    }

    @Override
    @PostMapping("/api/v1/order/{orderId}/return")
    public OrderDto productReturn(@PathVariable Long orderId) {
        return orderService.updateStatus(orderId, OrderStatus.PRODUCT_RETURNED);
    }

    @Override
    @PostMapping("/api/v1/order/{orderId}/cancel")
    public OrderDto cancel(@PathVariable Long orderId) {
        return orderService.updateStatus(orderId, OrderStatus.CANCELED);
    }
}

