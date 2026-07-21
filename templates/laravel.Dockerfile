FROM node:22-alpine AS assets

WORKDIR /app
COPY . .
RUN mkdir -p public/build && \
    if [ -f package.json ]; then \
      if [ -f package-lock.json ]; then npm ci; else npm install; fi && \
      npm run build; \
    fi

FROM composer:2 AS composer

WORKDIR /app
COPY . .
RUN composer install \
    --no-dev \
    --no-interaction \
    --no-progress \
    --no-scripts \
    --optimize-autoloader \
    --prefer-dist

FROM php:8.4-apache

RUN apt-get update && apt-get install -y --no-install-recommends \
      libicu-dev \
      libfreetype6-dev \
      libjpeg62-turbo-dev \
      libonig-dev \
      libpng-dev \
      libpq-dev \
      libzip-dev \
      unzip \
    && docker-php-ext-configure gd --with-freetype --with-jpeg \
    && docker-php-ext-install -j"$(nproc)" \
      bcmath \
      exif \
      gd \
      intl \
      mbstring \
      opcache \
      pcntl \
      pdo_mysql \
      pdo_pgsql \
      zip \
    && a2enmod rewrite \
    && sed -ri 's!DocumentRoot /var/www/html!DocumentRoot /var/www/html/public!g' /etc/apache2/sites-available/*.conf \
    && sed -ri 's!<Directory /var/www/>!<Directory /var/www/html/public>!g' /etc/apache2/apache2.conf \
    && sed -ri 's!AllowOverride None!AllowOverride All!g' /etc/apache2/apache2.conf \
    && sed -ri 's/Listen 80/Listen 3000/' /etc/apache2/ports.conf \
    && sed -ri 's/<VirtualHost \*:80>/<VirtualHost *:3000>/' /etc/apache2/sites-available/*.conf \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /var/www/html
COPY . .
COPY --from=composer /app/vendor ./vendor
COPY --from=assets /app/public/build ./public/build
COPY .deploynest/laravel-entrypoint.sh /usr/local/bin/deploynest-laravel

RUN chmod +x /usr/local/bin/deploynest-laravel \
    && mkdir -p storage/framework/cache storage/framework/sessions storage/framework/views bootstrap/cache database \
    && chown -R www-data:www-data storage bootstrap/cache database

EXPOSE 3000
ENTRYPOINT ["deploynest-laravel"]
