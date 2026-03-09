-- Database per Service: отдельные схемы для каждого микросервиса commerce
-- Выполняется в БД analyzer (общий сервер PostgreSQL)

CREATE SCHEMA IF NOT EXISTS store;
CREATE SCHEMA IF NOT EXISTS cart;
CREATE SCHEMA IF NOT EXISTS "order";
CREATE SCHEMA IF NOT EXISTS warehouse;
CREATE SCHEMA IF NOT EXISTS payment;
CREATE SCHEMA IF NOT EXISTS delivery;
