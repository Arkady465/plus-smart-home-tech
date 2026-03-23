package ru.yandex.practicum.commerce.store.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.commerce.dto.*;
import ru.yandex.practicum.commerce.store.entity.Product;
import ru.yandex.practicum.commerce.store.repository.ProductRepository;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class ProductService {

    private final ProductRepository productRepository;

    public List<ProductDto> getProducts(ProductCategory category) {
        List<Product> products = category != null
                ? productRepository.findByStateAndCategory(ProductState.ACTIVE, category)
                : productRepository.findByState(ProductState.ACTIVE);
        return products.stream().map(this::toDto).collect(Collectors.toList());
    }

    public ProductDto getProduct(Long id) {
        Product product = productRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("Product not found: " + id));
        return toDto(product);
    }

    @Transactional
    public ProductDto createProduct(ProductCreateUpdateDto dto) {
        List<String> photos = dto.getPhotos() != null && !dto.getPhotos().isEmpty()
                ? new ArrayList<>(dto.getPhotos())
                : new ArrayList<>();
        Product product = Product.builder()
                .name(dto.getName())
                .description(dto.getDescription())
                .photos(photos)
                .category(dto.getCategory())
                .quantity(0)
                .state(ProductState.ACTIVE)
                .build();
        product = productRepository.save(product);
        return toDto(product);
    }

    @Transactional
    public ProductDto updateProduct(Long id, ProductCreateUpdateDto dto) {
        Product product = productRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("Product not found: " + id));
        product.setName(dto.getName());
        product.setDescription(dto.getDescription());
        List<String> photos = dto.getPhotos() != null && !dto.getPhotos().isEmpty()
                ? new ArrayList<>(dto.getPhotos())
                : new ArrayList<>();
        product.setPhotos(photos);
        product.setCategory(dto.getCategory());
        product = productRepository.save(product);
        return toDto(product);
    }

    @Transactional
    public void deleteProduct(Long id) {
        Product product = productRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("Product not found: " + id));
        product.setState(ProductState.DEACTIVATE);
        productRepository.save(product);
    }

    @Transactional
    public ProductDto updateAvailability(Long id, ProductAvailability availability) {
        Product product = productRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("Product not found: " + id));
        product.setQuantity(availabilityToQuantity(availability));
        product = productRepository.save(product);
        return toDto(product);
    }

    public void setQuantity(Long id, int quantity) {
        Product product = productRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("Product not found: " + id));
        product.setQuantity(quantity);
        productRepository.save(product);
    }

    private ProductDto toDto(Product product) {
        return ProductDto.builder()
                .id(product.getId())
                .name(product.getName())
                .description(product.getDescription())
                .photos(product.getPhotos())
                .category(product.getCategory())
                .availability(quantityToAvailability(product.getQuantity()))
                .state(product.getState())
                .build();
    }

    private static ProductAvailability quantityToAvailability(int quantity) {
        if (quantity <= 0) return ProductAvailability.ENDED;
        if (quantity < 10) return ProductAvailability.FEW;
        if (quantity <= 100) return ProductAvailability.ENOUGH;
        return ProductAvailability.MANY;
    }

    private static int availabilityToQuantity(ProductAvailability availability) {
        return switch (availability) {
            case ENDED -> 0;
            case FEW -> 5;
            case ENOUGH -> 50;
            case MANY -> 150;
        };
    }
}
