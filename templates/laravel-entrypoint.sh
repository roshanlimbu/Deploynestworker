#!/bin/sh
set -eu

cd /var/www/html

if [ ! -f .env ] && [ -f .env.example ]; then
  cp .env.example .env
fi

mkdir -p storage/framework/cache storage/framework/sessions storage/framework/views bootstrap/cache database
chown -R www-data:www-data storage bootstrap/cache database

if [ -z "${APP_KEY:-}" ] && [ -f .env ] && ! grep -Eq '^APP_KEY=.+$' .env; then
  php artisan key:generate --force
fi

php artisan package:discover --ansi || true
php artisan storage:link || true

exec apache2-foreground
