package ru.yandex.practicum.commerce.warehouse.entity;

import jakarta.persistence.*;
import lombok.*;

@Entity
@Table(name = "order_booking_items")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OrderBookingItem {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "booking_id", nullable = false)
    private OrderBooking booking;

    @Column(nullable = false, length = 255)
    private String productId;

    @Column(nullable = false)
    private int quantity;
}

