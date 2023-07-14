<?php

$user = 'postgres';
$password = getenv('PGPASSWORD');
$host = getenv('PGHOST');
$port = getenv('PGPORT');

$data = explode("\n", file_get_contents(__DIR__ . '/../tests/data/00-init-databases-and-users.sql'));

$connection = new \PDO("pgsql:port=$port;host=$host;user=$user;password=$password");

foreach ($data as $query) {
    $connection->exec($query);
}