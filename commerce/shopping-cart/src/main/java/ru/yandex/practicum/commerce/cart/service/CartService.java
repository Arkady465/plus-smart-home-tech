package ru.yandex.practicum.commerce.cart.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.commerce.api.WarehouseClient;
import ru.yandex.practicum.commerce.dto.*;
import ru.yandex.practicum.commerce.cart.entity.Cart;
import ru.yandex.practicum.commerce.cart.entity.CartItem;
import ru.yandex.practicum.commerce.cart.repository.CartRepository;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Service
@RequiredArgsConstructor
public class CartService {

    private final CartRepository cartRepository;
    private final WarehouseClient warehouseClient;

    public CartDto getCart(String username) {
        Cart cart = cartRepository.findByUsername(username)
                .orElseGet(() -> createCart(username));
        return toDto(cart);
    }

    @Transactional
    public CartDto addItem(String username, CartItemDto itemDto) {
        Cart cart = cartRepository.findByUsername(username)
                .orElseGet(() -> createCart(username));

        if (cart.getState() == CartState.DEACTIVATED) {
            throw new IllegalStateException("Cannot add items to deactivated cart");
        }

        List<CartItemDto> itemsForCheck = new ArrayList<>(cart.getItems().stream()
                .map(i -> CartItemDto.builder().productId(i.getProductId()).quantity(i.getQuantity()).build())
                .toList());
        Optional<CartItem> existing = cart.getItems().stream()
                .filter(i -> i.getProductId().equals(itemDto.getProductId()))
                .findFirst();
        if (existing.isPresent()) {
            int newQty = existing.get().getQuantity() + itemDto.getQuantity();
            itemsForCheck.removeIf(i -> i.getProductId().equals(itemDto.getProductId()));
            itemsForCheck.add(CartItemDto.builder().productId(itemDto.getProductId()).quantity(newQty).build());
        } else {
            itemsForCheck.add(itemDto);
        }

        AvailabilityCheckResponseDto availability = warehouseClient.checkAvailability(
                AvailabilityCheckRequestDto.builder().items(itemsForCheck).build());
        if (!availability.isAvailable()) {
            throw new IllegalStateException("Insufficient stock for products: " + availability.getInsufficientProductIds());
        }

        Optional<CartItem> opt = cart.getItems().stream()
                .filter(i -> i.getProductId().equals(itemDto.getProductId()))
                .findFirst();
        if (opt.isPresent()) {
            opt.get().setQuantity(opt.get().getQuantity() + itemDto.getQuantity());
        } else {
            CartItem newItem = CartItem.builder()
                    .cart(cart)
                    .productId(itemDto.getProductId())
                    .quantity(itemDto.getQuantity())
                    .build();
            cart.getItems().add(newItem);
        }
        cart = cartRepository.save(cart);
        return toDto(cart);
    }

    @Transactional
    public CartDto updateItemQuantity(String username, Long productId, int quantity) {
        Cart cart = cartRepository.findByUsername(username)
                .orElseThrow(() -> new RuntimeException("Cart not found: " + username));
        if (cart.getState() == CartState.DEACTIVATED) {
            throw new IllegalStateException("Cannot modify deactivated cart");
        }
        if (quantity <= 0) {
            cart.getItems().removeIf(i -> i.getProductId().equals(productId));
        } else {
            cart.getItems().stream()
                    .filter(i -> i.getProductId().equals(productId))
                    .findFirst()
                    .ifPresent(i -> i.setQuantity(quantity));
        }
        cart = cartRepository.save(cart);
        return toDto(cart);
    }

    @Transactional
    public CartDto removeItem(String username, Long productId) {
        return updateItemQuantity(username, productId, 0);
    }

    @Transactional
    public CartDto deactivateCart(String username) {
        Cart cart = cartRepository.findByUsername(username)
                .orElseThrow(() -> new RuntimeException("Cart not found: " + username));
        cart.setState(CartState.DEACTIVATED);
        cart = cartRepository.save(cart);
        return toDto(cart);
    }

    private Cart createCart(String username) {
        Cart cart = Cart.builder().username(username).state(CartState.ACTIVE).build();
        return cartRepository.save(cart);
    }

    private CartDto toDto(Cart cart) {
        List<CartItemDto> items = cart.getItems().stream()
                .map(i -> CartItemDto.builder()
                        .productId(i.getProductId())
                        .quantity(i.getQuantity())
                        .build())
                .toList();
        return CartDto.builder()
                .id(cart.getId())
                .username(cart.getUsername())
                .state(cart.getState())
                .items(items)
                .build();
    }
}
