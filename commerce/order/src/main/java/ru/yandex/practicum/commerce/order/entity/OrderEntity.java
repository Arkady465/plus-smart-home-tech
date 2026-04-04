package ru.yandex.practicum.commerce.order.entity;

import jakarta.persistence.*;
import lombok.*;
import ru.yandex.practicum.commerce.dto.OrderStatus;

import java.util.ArrayList;
import java.util.List;

@Entity
@Table(name = "orders")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OrderEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String username;

    private Long cartId;
    private Long paymentId;
    private Long deliveryId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    @Builder.Default
    private OrderStatus status = OrderStatus.NEW;

    private Double totalCost;
    private Double productsCost;
    private Double deliveryCost;

    private Double weight;
    private Double volume;

    @Column(nullable = false)
    @Builder.Default
    private boolean fragile = false;

    @OneToMany(mappedBy = "order", cascade = CascadeType.ALL, orphanRemoval = true)
    @Builder.Default
    private List<OrderItemEntity> items = new ArrayList<>();
}

