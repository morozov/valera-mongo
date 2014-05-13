<?php

$loader = require __DIR__ . '/../vendor/autoload.php';
$loader->add('Valera\Tests', __DIR__);
$loader->add('Valera\Tests', __DIR__ . '/../vendor/valera/engine/tests');

if (is_readable(__DIR__.'/config.php')) {
    require_once __DIR__.'/config.php';
}
