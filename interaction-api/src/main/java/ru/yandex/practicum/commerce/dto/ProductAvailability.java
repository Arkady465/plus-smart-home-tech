package ru.yandex.practicum.commerce.dto;

/**
 * Степень доступности товара на витрине.
 */
public enum ProductAvailability {
    ENDED,   // товар закончился
    FEW,     // осталось меньше 10 единиц
    ENOUGH,  // от 10 до 100 единиц
    MANY     // больше 100 единиц
}
