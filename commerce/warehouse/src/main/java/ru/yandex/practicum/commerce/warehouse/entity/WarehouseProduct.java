package ru.yandex.practicum.commerce.warehouse.entity;

import jakarta.persistence.*;
import lombok.*;

@Entity
@Table(name = "warehouse_products")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class WarehouseProduct {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true, length = 64)
    private String productId;

    @Column(nullable = false)
    private int quantity;

    private Double width;
    private Double height;
    private Double depth;
    private Double weight;

    @Column(nullable = false)
    @Builder.Default
    private Boolean fragile = false;
}
