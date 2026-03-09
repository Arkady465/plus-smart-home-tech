package ru.yandex.practicum.commerce.cart.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.commerce.cart.model.Cart;
import ru.yandex.practicum.commerce.cart.model.CartItem;
import ru.yandex.practicum.commerce.cart.repository.CartRepository;

import java.util.Map;

@RestController
@RequestMapping
@RequiredArgsConstructor
public class CartController {

    private final CartRepository cartRepository;

    @GetMapping("/{userId}")
    public ResponseEntity<Cart> getCart(@PathVariable Long userId) {
        return cartRepository.findByUserId(userId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @PostMapping("/{userId}/items")
    public Cart addItem(@PathVariable Long userId, @RequestBody AddItemRequest request) {
        Cart cart = cartRepository.findByUserId(userId)
                .orElseGet(() -> cartRepository.save(Cart.builder().userId(userId).build()));
        CartItem item = CartItem.builder()
                .cart(cart)
                .productId(request.productId())
                .quantity(request.quantity())
                .build();
        cart.getItems().add(item);
        return cartRepository.save(cart);
    }

    @DeleteMapping("/{userId}/items/{productId}")
    public ResponseEntity<Cart> removeItem(@PathVariable Long userId, @PathVariable Long productId) {
        return cartRepository.findByUserId(userId)
                .filter(cart -> cart.getItems().removeIf(i -> i.getProductId().equals(productId)))
                .map(cartRepository::save)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    public record AddItemRequest(Long productId, Integer quantity) {}
}
