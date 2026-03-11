package ru.yandex.practicum.commerce.dto;

/**
 * Состояние корзины.
 */
public enum CartState {
    ACTIVE,      // можно добавлять товары
    DEACTIVATED  // только просмотр, нельзя добавлять
}
