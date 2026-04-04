package ru.yandex.practicum.commerce.delivery.service;

import jakarta.persistence.EntityNotFoundException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.commerce.api.OrderClient;
import ru.yandex.practicum.commerce.api.WarehouseClient;
import ru.yandex.practicum.commerce.delivery.entity.DeliveryEntity;
import ru.yandex.practicum.commerce.delivery.repository.DeliveryRepository;
import ru.yandex.practicum.commerce.dto.*;

@Slf4j
@Service
@RequiredArgsConstructor
public class DeliveryService {

    private final DeliveryRepository deliveryRepository;
    private final WarehouseClient warehouseClient;
    private final OrderClient orderClient;

    @Transactional
    public DeliveryDto plan(DeliveryDto request) {
        DeliveryEntity entity = DeliveryEntity.builder()
                .orderId(request.getOrderId())
                .weight(request.getWeight())
                .volume(request.getVolume())
                .fragile(request.isFragile())
                .status(DeliveryStatus.CREATED)
                .deliveryCost(request.getDeliveryCost())
                .build();
        if (request.getFrom() != null) {
            mapFrom(request.getFrom(), entity);
        }
        if (request.getTo() != null) {
            mapTo(request.getTo(), entity);
        }
        entity = deliveryRepository.save(entity);
        log.info("Delivery planned id={} orderId={} status={}", entity.getId(), entity.getOrderId(), entity.getStatus());
        return toDto(entity);
    }

    public DeliveryCostResponseDto cost(DeliveryCostRequestDto request) {
        double base = 5.0;
        double sum = base;

        double multiplier = 1.0;
        if (containsAddress2(request.getWarehouseAddress())) multiplier = 2.0;
        if (containsAddress1(request.getWarehouseAddress())) multiplier = 1.0;

        sum = base * multiplier + base;

        if (request.isFragile()) {
            sum = sum + (sum * 0.2);
        }

        double weight = request.getWeight() != null ? request.getWeight() : 0.0;
        double volume = request.getVolume() != null ? request.getVolume() : 0.0;
        sum = sum + (weight * 0.3);
        sum = sum + (volume * 0.2);

        String whStreet = request.getWarehouseAddress() != null ? request.getWarehouseAddress().getStreet() : null;
        String toStreet = request.getDeliveryAddress() != null ? request.getDeliveryAddress().getStreet() : null;
        if (whStreet != null && toStreet != null && !whStreet.equalsIgnoreCase(toStreet)) {
            sum = sum + (sum * 0.2);
        }

        return DeliveryCostResponseDto.builder().deliveryCost(sum).build();
    }

    @Transactional
    public DeliveryDto pickup(Long deliveryId) {
        DeliveryEntity entity = deliveryRepository.findById(deliveryId)
                .orElseThrow(() -> new EntityNotFoundException("Delivery not found: " + deliveryId));
        DeliveryStatus previous = entity.getStatus();
        entity.setStatus(DeliveryStatus.IN_PROGRESS);
        entity = deliveryRepository.save(entity);
        log.info("Delivery id={} orderId={} status transition {} -> {}", deliveryId, entity.getOrderId(), previous, DeliveryStatus.IN_PROGRESS);
        if (entity.getOrderId() != null) {
            warehouseClient.shippedToDelivery(ShippedToDeliveryRequestDto.builder()
                    .orderId(entity.getOrderId())
                    .deliveryId(entity.getId())
                    .build());
        }
        return toDto(entity);
    }

    @Transactional
    public DeliveryDto delivered(Long deliveryId) {
        DeliveryEntity entity = deliveryRepository.findById(deliveryId)
                .orElseThrow(() -> new EntityNotFoundException("Delivery not found: " + deliveryId));
        DeliveryStatus previous = entity.getStatus();
        entity.setStatus(DeliveryStatus.DELIVERED);
        entity = deliveryRepository.save(entity);
        log.info("Delivery id={} orderId={} status transition {} -> {}", deliveryId, entity.getOrderId(), previous, DeliveryStatus.DELIVERED);
        if (entity.getOrderId() != null) {
            orderClient.deliverySuccess(entity.getOrderId());
        }
        return toDto(entity);
    }

    @Transactional
    public DeliveryDto failed(Long deliveryId) {
        DeliveryEntity entity = deliveryRepository.findById(deliveryId)
                .orElseThrow(() -> new EntityNotFoundException("Delivery not found: " + deliveryId));
        DeliveryStatus previous = entity.getStatus();
        entity.setStatus(DeliveryStatus.FAILED);
        entity = deliveryRepository.save(entity);
        log.info("Delivery id={} orderId={} status transition {} -> {}", deliveryId, entity.getOrderId(), previous, DeliveryStatus.FAILED);
        if (entity.getOrderId() != null) {
            orderClient.deliveryFailed(entity.getOrderId());
        }
        return toDto(entity);
    }

    @Transactional
    public DeliveryDto cancel(Long deliveryId) {
        DeliveryEntity entity = deliveryRepository.findById(deliveryId)
                .orElseThrow(() -> new EntityNotFoundException("Delivery not found: " + deliveryId));
        DeliveryStatus previous = entity.getStatus();
        entity.setStatus(DeliveryStatus.CANCELLED);
        entity = deliveryRepository.save(entity);
        log.info("Delivery id={} orderId={} status transition {} -> {}", deliveryId, entity.getOrderId(), previous, DeliveryStatus.CANCELLED);
        return toDto(entity);
    }

    private static boolean containsAddress1(AddressDto a) {
        return contains(a, "ADDRESS_1");
    }

    private static boolean containsAddress2(AddressDto a) {
        return contains(a, "ADDRESS_2");
    }

    private static boolean contains(AddressDto a, String token) {
        if (a == null || token == null) return false;
        return (a.getCountry() != null && a.getCountry().contains(token))
                || (a.getCity() != null && a.getCity().contains(token))
                || (a.getStreet() != null && a.getStreet().contains(token))
                || (a.getHouse() != null && a.getHouse().contains(token))
                || (a.getApartment() != null && a.getApartment().contains(token))
                || (a.getFlat() != null && a.getFlat().contains(token));
    }

    private static void mapFrom(AddressDto from, DeliveryEntity entity) {
        entity.setFromCountry(from.getCountry());
        entity.setFromCity(from.getCity());
        entity.setFromStreet(from.getStreet());
        entity.setFromHouse(from.getHouse());
        entity.setFromApartment(from.getApartment());
        entity.setFromFlat(from.getFlat());
    }

    private static void mapTo(AddressDto to, DeliveryEntity entity) {
        entity.setToCountry(to.getCountry());
        entity.setToCity(to.getCity());
        entity.setToStreet(to.getStreet());
        entity.setToHouse(to.getHouse());
        entity.setToApartment(to.getApartment());
        entity.setToFlat(to.getFlat());
    }

    private DeliveryDto toDto(DeliveryEntity entity) {
        AddressDto from = AddressDto.builder()
                .country(entity.getFromCountry())
                .city(entity.getFromCity())
                .street(entity.getFromStreet())
                .house(entity.getFromHouse())
                .apartment(entity.getFromApartment())
                .flat(entity.getFromFlat())
                .build();
        AddressDto to = AddressDto.builder()
                .country(entity.getToCountry())
                .city(entity.getToCity())
                .street(entity.getToStreet())
                .house(entity.getToHouse())
                .apartment(entity.getToApartment())
                .flat(entity.getToFlat())
                .build();
        return DeliveryDto.builder()
                .id(entity.getId())
                .orderId(entity.getOrderId())
                .from(from)
                .to(to)
                .weight(entity.getWeight())
                .volume(entity.getVolume())
                .fragile(entity.isFragile())
                .status(entity.getStatus())
                .deliveryCost(entity.getDeliveryCost())
                .build();
    }
}

