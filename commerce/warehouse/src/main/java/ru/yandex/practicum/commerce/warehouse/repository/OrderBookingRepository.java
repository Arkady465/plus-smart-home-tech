package ru.yandex.practicum.commerce.warehouse.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ru.yandex.practicum.commerce.warehouse.entity.OrderBooking;

import java.util.Optional;

public interface OrderBookingRepository extends JpaRepository<OrderBooking, Long> {
    Optional<OrderBooking> findByOrderId(Long orderId);
}

