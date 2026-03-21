package ru.yandex.practicum.commerce.cart.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.commerce.api.ShoppingCartClient;
import ru.yandex.practicum.commerce.cart.dto.CartApiRequestDto;
import ru.yandex.practicum.commerce.cart.dto.CartItemApiDto;
import ru.yandex.practicum.commerce.cart.service.CartService;
import ru.yandex.practicum.commerce.dto.CartDto;
import ru.yandex.practicum.commerce.dto.CartItemDto;

@RestController
@RequiredArgsConstructor
public class CartController implements ShoppingCartClient {

    private final CartService cartService;

    // === Feign contract (ShoppingCartClient) ===
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

    // === API v1 endpoints ===
    @GetMapping("/api/v1/shopping-cart")
    public CartDto getCartApiV1(@RequestParam String username) {
        return cartService.getCart(username);
    }

    @PutMapping("/api/v1/shopping-cart")
    public CartDto updateCartApiV1(@RequestParam String username, @RequestBody(required = false) CartApiRequestDto body) {
        if (body != null && body.getItems() != null) {
            for (CartItemApiDto item : body.getItems()) {
                Long pid = toLong(item.getProductId());
                if (pid != null) {
                    cartService.addItem(username, CartItemDto.builder()
                            .productId(pid)
                            .quantity(item.getQuantity())
                            .build());
                }
            }
        }
        return cartService.getCart(username);
    }

    @PostMapping("/api/v1/shopping-cart/change-quantity")
    public CartDto changeQuantityApiV1(@RequestParam String username, @RequestBody CartItemApiDto item) {
        Long pid = toLong(item.getProductId());
        if (pid == null) return cartService.getCart(username);
        return cartService.updateItemQuantity(username, pid, item.getQuantity());
    }

    @PostMapping("/api/v1/shopping-cart/remove")
    public CartDto removeItemApiV1(@RequestParam String username, @RequestBody CartItemApiDto item) {
        Long pid = toLong(item.getProductId());
        if (pid == null) return cartService.getCart(username);
        return cartService.removeItem(username, pid);
    }

    @DeleteMapping("/api/v1/shopping-cart")
    public CartDto deactivateCartApiV1(@RequestParam String username) {
        return cartService.deactivateCart(username);
    }

    private static Long toLong(Object o) {
        if (o == null) return null;
        if (o instanceof Number) return ((Number) o).longValue();
        try {
            return Long.parseLong(o.toString());
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
