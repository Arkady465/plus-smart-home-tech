package ru.yandex.practicum.commerce.delivery.entity;

import jakarta.persistence.*;
import lombok.*;
import ru.yandex.practicum.commerce.dto.DeliveryStatus;

@Entity
@Table(name = "deliveries")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DeliveryEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private Long orderId;

    private String fromCountry;
    private String fromCity;
    private String fromStreet;
    private String fromHouse;
    private String fromApartment;
    private String fromFlat;

    private String toCountry;
    private String toCity;
    private String toStreet;
    private String toHouse;
    private String toApartment;
    private String toFlat;

    private Double weight;
    private Double volume;

    @Column(nullable = false)
    @Builder.Default
    private boolean fragile = false;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    @Builder.Default
    private DeliveryStatus status = DeliveryStatus.CREATED;

    private Double deliveryCost;
}

