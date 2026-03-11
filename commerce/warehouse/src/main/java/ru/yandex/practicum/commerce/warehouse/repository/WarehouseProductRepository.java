package ru.yandex.practicum.commerce.warehouse.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ru.yandex.practicum.commerce.warehouse.entity.WarehouseProduct;

import java.util.Optional;

public interface WarehouseProductRepository extends JpaRepository<WarehouseProduct, Long> {

    Optional<WarehouseProduct> findByProductId(Long productId);
}
