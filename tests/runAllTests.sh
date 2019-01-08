#!/usr/bin/env bash

cd $(dirname $0)
CWD=`pwd`
PHPUNIT=$CWD/vendor/phpunit/phpunit/composer/bin/phpunit

# ensure composer is installed
if [ ! -f composer.phar ]; then
    echo "Download and install composer to: $CWD"
    read -p "press Enter to continue: "
    curl -sS https://getcomposer.org/installer | php
fi

# ensure phpunit is installed
if [ ! -f $PHPUNIT ]; then
    echo "Install phpunit using composer: "
    read -p "press Enter to continue: "
    php composer.phar install
fi

# run all tests
$PHPUNIT $@ -- dal
