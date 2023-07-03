FROM alpine:3.16

# Config
ARG TARGETARCH
ARG PHP_VERSION=8.0.29-r0

# Install PHP, extensions and other dependencies
RUN apk add --update --no-cache \
    php8=$PHP_VERSION \
    php8-session=$PHP_VERSION \
    php8-opcache=$PHP_VERSION \
    php8-ctype=$PHP_VERSION \
    php8-pdo=$PHP_VERSION \
    php8-pdo_pgsql=$PHP_VERSION \
    php8-curl=$PHP_VERSION \
    php8-intl=$PHP_VERSION \
    php8-mbstring=$PHP_VERSION \
    php8-dom=$PHP_VERSION \
    php8-openssl=$PHP_VERSION \
    php8-bcmath=$PHP_VERSION \
    php8-xml=$PHP_VERSION \
    php8-simplexml=$PHP_VERSION \
    php8-xmlreader=$PHP_VERSION \
    php8-xmlwriter=$PHP_VERSION \
    php8-tokenizer=$PHP_VERSION \
    php8-iconv=$PHP_VERSION \
    php8-pear=$PHP_VERSION \
    php8-posix=$PHP_VERSION \
    php8-gettext=$PHP_VERSION \
    php8-sockets=$PHP_VERSION \
    php8-zip=$PHP_VERSION \
    # Phar is only used to run composer in the DEV env
    php8-phar=$PHP_VERSION \
    file \
    # Posix grep is required by spSourceCodeTyposTest
    grep \
    # Install "tail", used by bootstrap-php-fpm.sh to read app logs from pipe and
    # forward them to stdout (the busybox one is optimized for smaller size,
    # this one for performance)
    coreutils \
    # Required to import and download from 3rd parties via HTTPS
    ca-certificates \
    # Bash is required to run EMR cron scripts
    bash \
    # we use make as built tool/task runner
    make \
    # Debugging utils (not used by the app)
    postgresql-client

WORKDIR /dal
