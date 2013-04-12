drop database if exists pgx_test;
drop user if exists pgx;

create user pgx password 'secret';
create database pgx_test owner = pgx;
