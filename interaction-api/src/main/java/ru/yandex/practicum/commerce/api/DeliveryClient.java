package ru.yandex.practicum.commerce.api;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.commerce.dto.DeliveryCostRequestDto;
import ru.yandex.practicum.commerce.dto.DeliveryCostResponseDto;
import ru.yandex.practicum.commerce.dto.DeliveryDto;

@FeignClient(name = "delivery")
public interface DeliveryClient {

    @PostMapping("/api/v1/delivery")
    DeliveryDto planDelivery(@RequestBody DeliveryDto request);

    @PostMapping("/api/v1/delivery/cost")
    DeliveryCostResponseDto deliveryCost(@RequestBody DeliveryCostRequestDto request);

    @PostMapping("/api/v1/delivery/{deliveryId}/pickup")
    DeliveryDto pickup(@PathVariable Long deliveryId);

    @PostMapping("/api/v1/delivery/{deliveryId}/delivered")
    DeliveryDto delivered(@PathVariable Long deliveryId);

    @PostMapping("/api/v1/delivery/{deliveryId}/failed")
    DeliveryDto failed(@PathVariable Long deliveryId);

    @PostMapping("/api/v1/delivery/{deliveryId}/cancel")
    DeliveryDto cancel(@PathVariable Long deliveryId);
}

