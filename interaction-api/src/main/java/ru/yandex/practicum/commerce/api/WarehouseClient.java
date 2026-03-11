package ru.yandex.practicum.commerce.api;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.commerce.dto.AddressDto;
import ru.yandex.practicum.commerce.dto.AvailabilityCheckRequestDto;
import ru.yandex.practicum.commerce.dto.AvailabilityCheckResponseDto;
import ru.yandex.practicum.commerce.dto.WarehouseProductCreateDto;
import ru.yandex.practicum.commerce.dto.WarehouseProductDto;

import java.util.List;

@FeignClient(name = "warehouse", fallbackFactory = WarehouseClientFallback.class)
public interface WarehouseClient {

    @GetMapping("/admin/products")
    List<WarehouseProductDto> getAllProducts();

    @PostMapping("/admin/products")
    WarehouseProductDto addProduct(@RequestBody WarehouseProductCreateDto dto);

    @PostMapping("/admin/products/{productId}/replenish")
    WarehouseProductDto replenish(@PathVariable String productId, @RequestParam int quantity);

    @PostMapping("/availability/check")
    AvailabilityCheckResponseDto checkAvailability(@RequestBody AvailabilityCheckRequestDto request);

    @GetMapping("/address")
    AddressDto getAddress();
}
