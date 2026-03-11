package ru.yandex.practicum.commerce.warehouse.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.commerce.dto.AddressDto;
import ru.yandex.practicum.commerce.dto.WarehouseProductCreateDto;
import ru.yandex.practicum.commerce.dto.WarehouseProductDto;
import ru.yandex.practicum.commerce.warehouse.dto.WarehouseProductApiRequestDto;
import ru.yandex.practicum.commerce.warehouse.service.WarehouseService;

@RestController
@RequestMapping("/api/v1/warehouse")
@RequiredArgsConstructor
public class ApiV1WarehouseController {

    private final WarehouseService warehouseService;

    @PutMapping
    public WarehouseProductDto addProduct(@RequestBody WarehouseProductApiRequestDto dto) {
        return warehouseService.addProduct(toCreateDto(dto));
    }

    @PostMapping("/add")
    public WarehouseProductDto addProductPost(@RequestBody WarehouseProductApiRequestDto dto) {
        return warehouseService.addProduct(toCreateDto(dto));
    }

    @GetMapping("/address")
    public AddressDto getAddress() {
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
