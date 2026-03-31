package ru.yandex.practicum.commerce.warehouse.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.commerce.dto.*;
import ru.yandex.practicum.commerce.warehouse.entity.OrderBooking;
import ru.yandex.practicum.commerce.warehouse.entity.OrderBookingItem;
import ru.yandex.practicum.commerce.warehouse.entity.WarehouseProduct;
import ru.yandex.practicum.commerce.warehouse.repository.OrderBookingRepository;
import ru.yandex.practicum.commerce.warehouse.repository.WarehouseProductRepository;

import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
public class WarehouseService {

    private final WarehouseProductRepository repository;
    private final OrderBookingRepository bookingRepository;
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
    public WarehouseProductDto replenish(String productId, int quantity) {
        WarehouseProduct product = repository.findByProductId(productId)
                .orElseThrow(() -> new RuntimeException("Product not found in warehouse: " + productId));
        product.setQuantity(product.getQuantity() + quantity);
        product = repository.save(product);
        return toDto(product);
    }

    public AvailabilityCheckResponseDto checkAvailability(AvailabilityCheckRequestDto request) {
        List<Object> insufficient = new ArrayList<>();
        for (CartItemDto item : request.getItems()) {
            String pid = item.getProductId() != null ? String.valueOf(item.getProductId()) : null;
            if (pid == null) continue;
            int available = repository.findByProductId(pid)
                    .map(WarehouseProduct::getQuantity)
                    .orElse(0);
            if (available < item.getQuantity()) {
                insufficient.add(pid);
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

    @Transactional
    public OrderAssemblyResponseDto assemblyProductForOrder(OrderAssemblyRequestDto request) {
        if (request.getOrderId() == null) {
            throw new IllegalArgumentException("orderId is required");
        }
        if (bookingRepository.findByOrderId(request.getOrderId()).isPresent()) {
            return OrderAssemblyResponseDto.builder().orderId(request.getOrderId()).assembled(true).build();
        }
        if (request.getItems() == null || request.getItems().isEmpty()) {
            throw new IllegalArgumentException("items are required");
        }
        // Check availability and deduct
        for (OrderItemDto item : request.getItems()) {
            if (item == null || item.getProductId() == null) continue;
            WarehouseProduct p = repository.findByProductId(item.getProductId())
                    .orElseThrow(() -> new RuntimeException("Product not found in warehouse: " + item.getProductId()));
            if (p.getQuantity() < item.getQuantity()) {
                throw new IllegalStateException("Insufficient stock for product: " + item.getProductId());
            }
        }
        OrderBooking booking = OrderBooking.builder().orderId(request.getOrderId()).build();
        for (OrderItemDto item : request.getItems()) {
            if (item == null || item.getProductId() == null) continue;
            WarehouseProduct p = repository.findByProductId(item.getProductId()).orElseThrow();
            p.setQuantity(p.getQuantity() - item.getQuantity());
            repository.save(p);
            booking.getItems().add(OrderBookingItem.builder()
                    .booking(booking)
                    .productId(item.getProductId())
                    .quantity(item.getQuantity())
                    .build());
        }
        bookingRepository.save(booking);
        return OrderAssemblyResponseDto.builder().orderId(request.getOrderId()).assembled(true).build();
    }

    @Transactional
    public void shippedToDelivery(ShippedToDeliveryRequestDto request) {
        if (request.getOrderId() == null) {
            throw new IllegalArgumentException("orderId is required");
        }
        OrderBooking booking = bookingRepository.findByOrderId(request.getOrderId())
                .orElseThrow(() -> new RuntimeException("Order booking not found: " + request.getOrderId()));
        booking.setDeliveryId(request.getDeliveryId());
        bookingRepository.save(booking);
    }

    @Transactional
    public void returnProducts(ProductReturnRequestDto request) {
        if (request.getItems() == null) return;
        for (OrderItemDto item : request.getItems()) {
            if (item == null || item.getProductId() == null) continue;
            WarehouseProduct p = repository.findByProductId(item.getProductId())
                    .orElseThrow(() -> new RuntimeException("Product not found in warehouse: " + item.getProductId()));
            p.setQuantity(p.getQuantity() + item.getQuantity());
            repository.save(p);
        }
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
