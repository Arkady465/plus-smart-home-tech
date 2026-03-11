package ru.yandex.practicum.commerce.store.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.commerce.api.ShoppingStoreClient;
import ru.yandex.practicum.commerce.dto.*;
import ru.yandex.practicum.commerce.store.service.ProductService;

import java.util.List;

@RestController
@RequiredArgsConstructor
public class ProductController implements ShoppingStoreClient {

    private final ProductService productService;

    @Override
    @GetMapping("/products")
    public List<ProductDto> getProducts(@RequestParam(required = false) ProductCategory category) {
        return productService.getProducts(category);
    }

    @Override
    @GetMapping("/products/{id}")
    public ProductDto getProduct(@PathVariable Long id) {
        return productService.getProduct(id);
    }

    @Override
    @PostMapping("/admin/products")
    public ProductDto createProduct(@RequestBody ProductCreateUpdateDto dto) {
        return productService.createProduct(dto);
    }

    @Override
    @PutMapping("/admin/products/{id}")
    public ProductDto updateProduct(@PathVariable Long id, @RequestBody ProductCreateUpdateDto dto) {
        return productService.updateProduct(id, dto);
    }

    @Override
    @DeleteMapping("/admin/products/{id}")
    public void deleteProduct(@PathVariable Long id) {
        productService.deleteProduct(id);
    }

    @Override
    @PutMapping("/admin/products/{id}/availability")
    public ProductDto updateAvailability(@PathVariable Long id, @RequestParam ProductAvailability availability) {
        return productService.updateAvailability(id, availability);
    }
}
