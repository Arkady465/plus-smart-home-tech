package ru.yandex.practicum.commerce.api;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.commerce.dto.OrderCreateRequestDto;
import ru.yandex.practicum.commerce.dto.OrderDto;

import java.util.List;

@FeignClient(name = "order")
public interface OrderClient {

    @PostMapping("/api/v1/order")
    OrderDto createOrder(@RequestBody OrderCreateRequestDto request);

    @GetMapping("/api/v1/order")
    List<OrderDto> getOrders(@RequestParam String username);

    @PostMapping("/api/v1/order/{orderId}/payment/success")
    OrderDto paymentSuccess(@PathVariable Long orderId);

    @PostMapping("/api/v1/order/{orderId}/payment/failed")
    OrderDto paymentFailed(@PathVariable Long orderId);

    @PostMapping("/api/v1/order/{orderId}/delivery/success")
    OrderDto deliverySuccess(@PathVariable Long orderId);

    @PostMapping("/api/v1/order/{orderId}/delivery/failed")
    OrderDto deliveryFailed(@PathVariable Long orderId);

    @PostMapping("/api/v1/order/{orderId}/assembly/failed")
    OrderDto assemblyFailed(@PathVariable Long orderId);

    @PostMapping("/api/v1/order/{orderId}/return")
    OrderDto productReturn(@PathVariable Long orderId);

    @PostMapping("/api/v1/order/{orderId}/cancel")
    OrderDto cancel(@PathVariable Long orderId);
}

