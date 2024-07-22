<?php

use SiteCore\Tools\Lock;
use SiteCore\Tools\Telegram;

const MAX_LISTEN_ITERATION = 100;
ini_set('memory_limit', '1G');

require_once __DIR__ . '/bx_prolog.php';
\Bitrix\Main\Loader::includeModule('logs');

if (!Lock::lock('RabbitListener')) {
    die();
}

try {
    $rb = new SiteCore\Rabbit\RabbitReceiver();
    $counter = 1;
    while ($counter < MAX_LISTEN_ITERATION) {
        foreach ($rb->rabbit::QUEUE_CONNECTOR as $queue => $param) {
            foreach ($param['LISTEN_ROUTE'] as $route) {
                while (ob_end_clean()) ;
                $rb->listen($queue, $route);
            }
        }
        $counter++;
    }
} catch (\PhpAmqpLib\Exception\AMQPIOException|Exception $e) {
    $error = 'Выброшено исключение: ' . $e->getMessage() . "\n" . 'Скрипт чтения очередей перезапущен';
    (new Telegram())->send(
        'RabbitBot',
        'RabbitMessages',
        print_r(get_class($e) . '. ' . $error, true),
        'RabbitError'
    );
    exit();
}
