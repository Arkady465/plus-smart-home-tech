package ru.yandex.practicum.commerce.delivery.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.commerce.delivery.model.Delivery;
import ru.yandex.practicum.commerce.delivery.repository.DeliveryRepository;

import java.util.List;

@RestController
@RequestMapping
@RequiredArgsConstructor
public class DeliveryController {

    private final DeliveryRepository deliveryRepository;

    @PostMapping
    public Delivery create(@RequestBody CreateDeliveryRequest request) {
        Delivery delivery = Delivery.builder()
                .orderId(request.orderId())
                .address(request.address())
                .status(Delivery.DeliveryStatus.PENDING)
                .build();
        return deliveryRepository.save(delivery);
    }

    @GetMapping("/order/{orderId}")
    public ResponseEntity<Delivery> getByOrderId(@PathVariable Long orderId) {
        return deliveryRepository.findByOrderId(orderId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping
    public List<Delivery> getAll() {
        return deliveryRepository.findAll();
    }

    public record CreateDeliveryRequest(Long orderId, String address) {}
}
