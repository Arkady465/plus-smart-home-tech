package ru.yandex.practicum.commerce.delivery.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.commerce.api.DeliveryClient;
import ru.yandex.practicum.commerce.delivery.service.DeliveryService;
import ru.yandex.practicum.commerce.dto.DeliveryCostRequestDto;
import ru.yandex.practicum.commerce.dto.DeliveryCostResponseDto;
import ru.yandex.practicum.commerce.dto.DeliveryDto;

@RestController
@RequiredArgsConstructor
public class DeliveryController implements DeliveryClient {

    private final DeliveryService deliveryService;

    @Override
    @PostMapping("/api/v1/delivery")
    public DeliveryDto planDelivery(@RequestBody DeliveryDto request) {
        return deliveryService.plan(request);
    }

    @Override
    @PostMapping("/api/v1/delivery/cost")
    public DeliveryCostResponseDto deliveryCost(@RequestBody DeliveryCostRequestDto request) {
        return deliveryService.cost(request);
    }

    @Override
    @PostMapping("/api/v1/delivery/{deliveryId}/pickup")
    public DeliveryDto pickup(@PathVariable Long deliveryId) {
        return deliveryService.pickup(deliveryId);
    }

    @Override
    @PostMapping("/api/v1/delivery/{deliveryId}/delivered")
    public DeliveryDto delivered(@PathVariable Long deliveryId) {
        return deliveryService.delivered(deliveryId);
    }

    @Override
    @PostMapping("/api/v1/delivery/{deliveryId}/failed")
    public DeliveryDto failed(@PathVariable Long deliveryId) {
        return deliveryService.failed(deliveryId);
    }

    @Override
    @PostMapping("/api/v1/delivery/{deliveryId}/cancel")
    public DeliveryDto cancel(@PathVariable Long deliveryId) {
        return deliveryService.cancel(deliveryId);
    }
}

