<?php

namespace SiteCore\Rabbit;

use SiteCore\Corebus\Parser;
use SiteCore\Elastic\ElasticHelper;
use SiteCore\Tools\DataAlteration;
use SiteCore\Tools\Telegram;
use SiteCore\Core;
use \PhpAmqpLib\Connection\AMQPConnection;
use \PhpAmqpLib\Message\AMQPMessage;

/**
 * @package SiteCore\Rabbit
 */
class RabbitReceiver
{
    private string $queue;
    private float $timerStart;
    public RabbitHelper $rabbit;

    public function __construct()
    {
        $this->rabbit = new RabbitHelper;
        $this->elastic = new ElasticHelper;
    }

    /**
     * Обрабатывает входящие запросы
     */
    public function listen(string $queue, string $route)
    {
        try {
            $queue .= DataAlteration::isDev() ? '_test' : '_prod';
            $exchange = DataAlteration::isDev() ? 'test' : 'prod';
            $this->timerStart = microtime(true);
            $this->queue = $queue;
            $rabbit = new RabbitHelper;
            $rabbit->channel->exchange_declare($exchange, 'direct', false, true, false);

            $cnt = $rabbit->channel->queue_declare($queue, true, false, false, false);

            if (!$cnt[1]) {
                return;
            }

            $rabbit->channel->queue_bind($queue, $exchange, $route);
            /**
             * Не отправляем новое сообщение на обработчик, пока он
             * не обработал и не подтвердил предыдущее. Вместо этого
             * направляем сообщение на любой свободный обработчик
             */
            $rabbit->channel->basic_qos(null, 1, null);
            /* получаем сообщение из очереди */
            $message = $rabbit->channel->basic_get($queue);
            if ($message->body) {
                //Подтверждаем успешное получение сообщение сразу. Не учитываем успешность обработки, оставляем это
                //на совести дальнейшего кода, в случае проблем пишем в эластик.
                $message->ack();

                //для дебага, чтобы выводились сообщения из бесконечного цикла в консоли
                //while (ob_end_clean());

                Core::addLog(
                    'rabbit',
                    'rabbit',
                    [
                        'time' => date("H:i:s"),
                        'route' => $route,
                        'queue' => $queue,
                        'message' => $message->body, //экономия места для логов
                    ]
                );

                $rawData = json_decode($message->body, JSON_UNESCAPED_UNICODE);

                $count = count($rawData);
                $i = 0;

                $mess = "Очередь: " . $this->queue . ": " . $route . "\nКоличество записей: " . $count . "\n";
                $mess .= "Обработка начата";

                (new Telegram())->send('RabbitBot', 'RabbitMessages', $mess, 'RabbitNewMessage');

                $integrationId = [];
                foreach ($rawData as $data) {
                    $error = "";
                    $xml = $data['xml'];
                    $integrationId[$i] = $data['digest']['integrationId'];
                    print_r(
                        ($i + 1) . " / " . $count . " integrationId: " . $integrationId[$i] . " q=" .
                        $this->queue . " r=" . $route . "\n"
                    );
                    while (ob_end_clean()) ;

                    try {
                        $simpleXml = new \SimpleXMLElement($xml);
                    } catch (Exception $e) {
                        /**
                         * TODO elastic
                         */
                        $error = 'Выброшено исключение: ' . $e->getMessage() . "\n";
                        (new Telegram())->send('RabbitBot', 'VRabbitMessages', print_r($error, true), 'RabbitError');
                    }

                    if (empty($error)) {
                        try {
                            $rabbitParser = new Parser();
                            $rabbitParser->processObject($simpleXml, $integrationId[$i]);
                        } catch (Exception $e) {
                            $error = 'Выброшено исключение: ' . $e->getMessage() . "\n";
                        }
                    }

                    if (empty($error)) {
                        $this->elastic->elasticLog(
                            json_encode([(object)$data]), $queue, $route, 'INPUT', 'Success'
                        );
                    } else {
                        $this->elastic->elasticLog(
                            json_encode([(object)$data]), $queue, $route, 'INPUT', 'Error', $error
                        );
                    }

                    $i++;
                    Core::addLog(
                        'rabbitIntegrationIds',
                        'rabbit',
                        [
                            'time' => date("H:i:s"),
                            'integrationId' => $integrationId[$i],
                        ]
                    );
                }

                $mess = "Очередь: " . $this->queue . ": " . $route . "\nЗаписи успешно обработаны.\n";
                $mess .= "id: " . implode("\n", array_slice($integrationId, 0, 10));
                $mess .= ($count > 10) ? "\n..." : "";

                (new Telegram())->send(
                    'RabbitBot',
                    'RabbitMessages',
                    substr($mess, 0, 4000),
                    'RabbitNewMessage'
                );

            }

            try {
                $rabbit->channel->close();
                $rabbit->connection->close();
            } catch (\Exception $e) {
                var_dump($e);
                while (ob_end_clean()) ;
            }
        } catch (\Exception $e) {
            var_dump($e);
            while (ob_end_clean()) ;
            /**
             * TODO elastic
             */
            (new Telegram())->send('RabbitBot', 'RabbitMessages', print_r($e, true), 'RabbitError');
        }
    }
}