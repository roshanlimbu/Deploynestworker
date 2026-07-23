#!/bin/sh
set -eu

cd /var/www/html

if [ ! -f .env ] && [ -f .env.example ]; then
  cp .env.example .env
  chown www-data:www-data .env
fi

mkdir -p storage/framework/cache storage/framework/sessions storage/framework/views bootstrap/cache database storage/logs
chown -R www-data:www-data storage bootstrap/cache database

if [ -z "${APP_KEY:-}" ] && [ -f .env ] && ! grep -Eq '^APP_KEY=.+$' .env; then
  su -s /bin/sh www-data -c "php artisan key:generate --force"
fi

su -s /bin/sh www-data -c "php artisan package:discover --ansi" || true
su -s /bin/sh www-data -c "php artisan storage:link" || true

# Just to be absolutely sure all created files (like logs) are correct
chown -R www-data:www-data storage bootstrap/cache database

exec apache2-foreground
