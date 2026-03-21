package ru.yandex.practicum.commerce.warehouse.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.commerce.api.WarehouseClient;
import ru.yandex.practicum.commerce.dto.*;
import ru.yandex.practicum.commerce.warehouse.dto.WarehouseProductApiRequestDto;
import ru.yandex.practicum.commerce.warehouse.service.WarehouseService;

import java.util.List;

@RestController
@RequiredArgsConstructor
public class WarehouseController implements WarehouseClient {

    private final WarehouseService warehouseService;

    // === Feign contract (WarehouseClient) ===
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

    // === API v1 endpoints ===
    @PutMapping("/api/v1/warehouse")
    public WarehouseProductDto addProductPutApiV1(@RequestBody WarehouseProductApiRequestDto dto) {
        return warehouseService.addProduct(toCreateDto(dto));
    }

    @PostMapping("/api/v1/warehouse/add")
    public WarehouseProductDto addProductPostApiV1(@RequestBody WarehouseProductApiRequestDto dto) {
        return warehouseService.addProduct(toCreateDto(dto));
    }

    @GetMapping("/api/v1/warehouse/address")
    public AddressDto getAddressApiV1() {
        return warehouseService.getAddress();
    }

    private WarehouseProductCreateDto toCreateDto(WarehouseProductApiRequestDto dto) {
        Double width = null, height = null, depth = null, weight = null;
        if (dto.getDimension() != null) {
            if (dto.getDimension().getWidth() != null) width = Double.parseDouble(dto.getDimension().getWidth());
            if (dto.getDimension().getHeight() != null) height = Double.parseDouble(dto.getDimension().getHeight());
            if (dto.getDimension().getDepth() != null) depth = Double.parseDouble(dto.getDimension().getDepth());
        }
        if (dto.getWeight() != null) weight = Double.parseDouble(dto.getWeight());
        Boolean fragile = "true".equalsIgnoreCase(dto.getFragile());
        return WarehouseProductCreateDto.builder()
                .productId(dto.getProductId())
                .quantity(1)
                .width(width)
                .height(height)
                .depth(depth)
                .weight(weight)
                .fragile(fragile)
                .build();
    }
}
