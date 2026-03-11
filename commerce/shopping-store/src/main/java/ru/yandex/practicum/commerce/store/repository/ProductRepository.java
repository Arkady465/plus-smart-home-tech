package ru.yandex.practicum.commerce.store.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ru.yandex.practicum.commerce.dto.ProductCategory;
import ru.yandex.practicum.commerce.dto.ProductState;
import ru.yandex.practicum.commerce.store.entity.Product;

import java.util.List;

public interface ProductRepository extends JpaRepository<Product, Long> {

    List<Product> findByStateAndCategory(ProductState state, ProductCategory category);

    List<Product> findByState(ProductState state);
}
