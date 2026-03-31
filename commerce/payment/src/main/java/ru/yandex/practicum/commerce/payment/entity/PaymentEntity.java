package ru.yandex.practicum.commerce.payment.entity;

import jakarta.persistence.*;
import lombok.*;
import ru.yandex.practicum.commerce.dto.PaymentStatus;

@Entity
@Table(name = "payments")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class PaymentEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private Long orderId;

    private Double productsCost;
    private Double deliveryCost;
    private Double totalCost;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    @Builder.Default
    private PaymentStatus status = PaymentStatus.PENDING;
}

