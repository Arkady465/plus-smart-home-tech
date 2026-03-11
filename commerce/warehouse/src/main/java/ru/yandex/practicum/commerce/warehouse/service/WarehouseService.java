package ru.yandex.practicum.commerce.warehouse.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.commerce.dto.*;
import ru.yandex.practicum.commerce.warehouse.entity.WarehouseProduct;
import ru.yandex.practicum.commerce.warehouse.repository.WarehouseProductRepository;

import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
public class WarehouseService {

    private final WarehouseProductRepository repository;
    private final AddressDto currentAddress;

    public List<WarehouseProductDto> getAllProducts() {
        return repository.findAll().stream().map(this::toDto).toList();
    }

    @Transactional
    public WarehouseProductDto addProduct(WarehouseProductCreateDto dto) {
        WarehouseProduct product = repository.findByProductId(dto.getProductId())
                .orElse(null);
        if (product != null) {
            product.setQuantity(product.getQuantity() + dto.getQuantity());
            product.setWidth(dto.getWidth());
            product.setHeight(dto.getHeight());
            product.setDepth(dto.getDepth());
            product.setWeight(dto.getWeight());
            product.setFragile(dto.getFragile() != null ? dto.getFragile() : false);
        } else {
            product = WarehouseProduct.builder()
                    .productId(dto.getProductId())
                    .quantity(dto.getQuantity())
                    .width(dto.getWidth())
                    .height(dto.getHeight())
                    .depth(dto.getDepth())
                    .weight(dto.getWeight())
                    .fragile(dto.getFragile() != null ? dto.getFragile() : false)
                    .build();
        }
        product = repository.save(product);
        return toDto(product);
    }

    @Transactional
    public WarehouseProductDto replenish(Long productId, int quantity) {
        WarehouseProduct product = repository.findByProductId(productId)
                .orElseThrow(() -> new RuntimeException("Product not found in warehouse: " + productId));
        product.setQuantity(product.getQuantity() + quantity);
        product = repository.save(product);
        return toDto(product);
    }

    public AvailabilityCheckResponseDto checkAvailability(AvailabilityCheckRequestDto request) {
        List<Long> insufficient = new ArrayList<>();
        for (CartItemDto item : request.getItems()) {
            int available = repository.findByProductId(item.getProductId())
                    .map(WarehouseProduct::getQuantity)
                    .orElse(0);
            if (available < item.getQuantity()) {
                insufficient.add(item.getProductId());
            }
        }
        return AvailabilityCheckResponseDto.builder()
                .available(insufficient.isEmpty())
                .insufficientProductIds(insufficient)
                .build();
    }

    public AddressDto getAddress() {
        return currentAddress;
    }

    private WarehouseProductDto toDto(WarehouseProduct p) {
        return WarehouseProductDto.builder()
                .id(p.getId())
                .productId(p.getProductId())
                .quantity(p.getQuantity())
                .width(p.getWidth())
                .height(p.getHeight())
                .depth(p.getDepth())
                .weight(p.getWeight())
                .fragile(p.getFragile())
                .build();
    }
}
