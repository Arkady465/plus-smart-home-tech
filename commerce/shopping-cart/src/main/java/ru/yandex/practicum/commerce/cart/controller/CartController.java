package ru.yandex.practicum.commerce.cart.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.commerce.api.ShoppingCartClient;
import ru.yandex.practicum.commerce.dto.CartDto;
import ru.yandex.practicum.commerce.dto.CartItemDto;
import ru.yandex.practicum.commerce.cart.service.CartService;

@RestController
@RequiredArgsConstructor
public class CartController implements ShoppingCartClient {

    private final CartService cartService;

    @Override
    @GetMapping("/carts/{username}")
    public CartDto getCart(@PathVariable String username) {
        return cartService.getCart(username);
    }

    @Override
    @PostMapping("/carts/{username}/items")
    public CartDto addItem(@PathVariable String username, @RequestBody CartItemDto item) {
        return cartService.addItem(username, item);
    }

    @Override
    @PutMapping("/carts/{username}/items/{productId}")
    public CartDto updateItemQuantity(@PathVariable String username, @PathVariable Long productId,
                                      @RequestParam int quantity) {
        return cartService.updateItemQuantity(username, productId, quantity);
    }

    @Override
    @DeleteMapping("/carts/{username}/items/{productId}")
    public CartDto removeItem(@PathVariable String username, @PathVariable Long productId) {
        return cartService.removeItem(username, productId);
    }

    @Override
    @PostMapping("/carts/{username}/deactivate")
    public CartDto deactivateCart(@PathVariable String username) {
        return cartService.deactivateCart(username);
    }
}
