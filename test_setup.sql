drop database if exists pgx_test;
drop user if exists pgx_none;
drop user if exists pgx_pw;
drop user if exists pgx_md5;

create user pgx_none;
create user pgx_pw password 'secret';
create user pgx_md5 password 'secret';
create database pgx_test;
