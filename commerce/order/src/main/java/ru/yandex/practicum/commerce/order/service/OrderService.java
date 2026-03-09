package ru.yandex.practicum.commerce.order.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.commerce.order.client.*;
import ru.yandex.practicum.commerce.order.dto.CartDto;
import ru.yandex.practicum.commerce.order.dto.CartItemDto;
import ru.yandex.practicum.commerce.order.dto.ProductDto;
import ru.yandex.practicum.commerce.order.model.Order;
import ru.yandex.practicum.commerce.order.model.OrderItem;
import ru.yandex.practicum.commerce.order.model.Order.OrderStatus;
import ru.yandex.practicum.commerce.order.repository.OrderRepository;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
public class OrderService {

    private final OrderRepository orderRepository;
    private final CartClient cartClient;
    private final StoreClient storeClient;
    private final WarehouseClient warehouseClient;
    private final PaymentClient paymentClient;
    private final DeliveryClient deliveryClient;

    @Transactional
    public Order createOrder(Long userId, String deliveryAddress) {
        CartDto cart = cartClient.getCart(userId);
        if (cart == null || cart.getItems() == null || cart.getItems().isEmpty()) {
            throw new IllegalArgumentException("Корзина пуста");
        }

        List<OrderItem> orderItems = new ArrayList<>();
        BigDecimal totalAmount = BigDecimal.ZERO;

        for (CartItemDto cartItem : cart.getItems()) {
            ProductDto product = storeClient.getProduct(cartItem.getProductId());
            if (product == null) {
                throw new IllegalArgumentException("Товар не найден: " + cartItem.getProductId());
            }
            warehouseClient.reserve(cartItem.getProductId(), cartItem.getQuantity());

            BigDecimal itemTotal = product.getPrice().multiply(BigDecimal.valueOf(cartItem.getQuantity()));
            totalAmount = totalAmount.add(itemTotal);

            OrderItem orderItem = OrderItem.builder()
                    .productId(cartItem.getProductId())
                    .quantity(cartItem.getQuantity())
                    .price(product.getPrice())
                    .build();
            orderItems.add(orderItem);
        }

        Order order = Order.builder()
                .userId(userId)
                .status(OrderStatus.CREATED)
                .items(orderItems)
                .build();
        orderItems.forEach(item -> item.setOrder(order));
        order = orderRepository.save(order);

        paymentClient.processPayment(order.getId(), totalAmount);
        deliveryClient.createDelivery(order.getId(), deliveryAddress);

        order.setStatus(OrderStatus.CONFIRMED);
        return orderRepository.save(order);
    }

    public Order getOrder(Long id) {
        return orderRepository.findById(id).orElseThrow();
    }

    public List<Order> getOrdersByUserId(Long userId) {
        return orderRepository.findByUserIdOrderByIdDesc(userId);
    }
}
