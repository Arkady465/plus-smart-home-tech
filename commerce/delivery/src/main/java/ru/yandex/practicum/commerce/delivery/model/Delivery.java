package ru.yandex.practicum.commerce.delivery.model;

import jakarta.persistence.*;
import lombok.*;

@Entity
@Table(name = "deliveries", schema = "delivery")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Delivery {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private Long orderId;

    @Column(nullable = false)
    private String address;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private DeliveryStatus status;

    public enum DeliveryStatus {
        PENDING, SHIPPED, IN_TRANSIT, DELIVERED
    }
}
