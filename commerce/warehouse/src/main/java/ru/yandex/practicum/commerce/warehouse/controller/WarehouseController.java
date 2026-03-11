package ru.yandex.practicum.commerce.warehouse.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.commerce.api.WarehouseClient;
import ru.yandex.practicum.commerce.dto.*;
import ru.yandex.practicum.commerce.warehouse.service.WarehouseService;

import java.util.List;

@RestController
@RequiredArgsConstructor
public class WarehouseController implements WarehouseClient {

    private final WarehouseService warehouseService;

    @Override
    @GetMapping("/admin/products")
    public List<WarehouseProductDto> getAllProducts() {
        return warehouseService.getAllProducts();
    }

    @Override
    @PostMapping("/admin/products")
    public WarehouseProductDto addProduct(@RequestBody WarehouseProductCreateDto dto) {
        return warehouseService.addProduct(dto);
    }

    @Override
    @PostMapping("/admin/products/{productId}/replenish")
    public WarehouseProductDto replenish(@PathVariable String productId, @RequestParam int quantity) {
        return warehouseService.replenish(productId, quantity);
    }

    @Override
    @PostMapping("/availability/check")
    public AvailabilityCheckResponseDto checkAvailability(@RequestBody AvailabilityCheckRequestDto request) {
        return warehouseService.checkAvailability(request);
    }

    @Override
    @GetMapping("/address")
    public AddressDto getAddress() {
        return warehouseService.getAddress();
    }
}
