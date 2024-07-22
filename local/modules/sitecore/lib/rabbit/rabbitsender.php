<?php

namespace SiteCore\Rabbit;

use SiteCore\Core;
use SiteCore\Elastic\ElasticHelper;
use \PhpAmqpLib\Connection\AMQPConnection;
use \PhpAmqpLib\Message\AMQPMessage;
use Sitecore\Tools\Telegram;
use SiteCore\Tools\DataAlteration;

/**
 * @package SiteCore\Rabbit
 */
class RabbitSender
{
    public RabbitHelper $rabbit;
    public ElasticHelper $elastic;

    public function __construct()
    {
        $this->rabbit = new RabbitHelper;
        $this->elastic = new ElasticHelper;
    }

    /**
     * Отправляет задачу на генерацию накладной обработчикам
     *
     * @param string $queue Наименование очереди
     * @param string $mess Сообщение
     * @throws \Exception
     */
    public function execute(string $queue, string $mess, string $routeKey)
    {
        $queue .= DataAlteration::isDev() ? '_test' : '_prod';
        $exchange = DataAlteration::isDev() ? 'test' : 'prod';

        $this->rabbit->connect();
        $this->rabbit->channel = $this->rabbit->connection->channel();
        $this->rabbit->channel->exchange_declare($exchange, 'direct', false, true, false);
        $this->rabbit->channel->queue_declare($queue, true, false, false, false);
        $msg = new AMQPMessage($mess);
        $this->rabbit->channel->queue_bind($queue, $exchange, $routeKey);
        $this->rabbit->channel->basic_publish($msg, $exchange, $routeKey);


        $messages = $this->elastic->elasticLog($mess, $queue, $routeKey, 'OUTPUT', 'Success');

        Core::addLog(
            'sendMSG',
            'rabbit',
            [
                'time' => date("H:i:s"),
                'queue' => $queue,
                'routeKey' => $routeKey,
                'msg' => $msg,
            ]
        );

        (new Telegram())->send(
            'RabbitBot',
            'RabbitMessages',
            'Отправлено сообщение в ' . $queue . ' : ' . $routeKey . ' (cnt=' . count($messages) . ')', 'RabbitNewMessageSend'
        );
    }

    private function prepareDate(array $data): array
    {
        $result = [];

        foreach ($data['ReadIntegrationMessagesResult'] as $message) {
            $result[$message->digest->queue][] = $message;
        }

        foreach ($result as $queue => $res) {
            $queueName = $this->rabbit::QUEUE_CONNECTOR[$queue];

            if (!$queueName) {
                (new Telegram())->send(
                    'RabbitBot', 'RabbitMessages',
                    'Неизвестная очередь ' . $queue, 'RabbitError'
                );
                return [];
            }

            foreach ($res as &$tmpRes) {
                $recipientAdapters = json_decode(json_encode($tmpRes->digest->recipientAdapters), true);
                $tmpRes->digest->recipientAdapters = $recipientAdapters['int'];
                unset($tmpRes->digest->queue);
            }
            unset($tmpRes);
        }

        return $result;
    }

    /**
     * Выгружаем даннные в Rabbit
     * @param array $data
     * @return bool
     */
    public function sendMessages(array $data): bool
    {
        $isSuccess = true;
        $result = $this->prepareDate($data);
        try {
            foreach ($result as $queue => $res) {
                $queueName = $this->rabbit::QUEUE_CONNECTOR[$queue];
                $chunkedRes = array_chunk($res, $this->rabbit::MESSAGE_LIMIT);

                foreach ($chunkedRes as $chunk) {
                    foreach ($queueName['PUBLISH_QUEUE'] as $queue) {
                        $this->execute($queue, json_encode($chunk), $queueName['PUBLISH_ROUTE']);
                    }

                    /*todo убрать после отладки, заменить на лог в эластик elastic*/
                    if (!$queueName['PUBLISH_QUEUE']) {
                        $this->elastic->elasticLog(
                            json_encode($chunk),
                            $queue,
                            $queueName['PUBLISH_ROUTE'],
                            'OUTPUT',
                            'Error',
                            'Не добавлено в очередь ' . $queueName['PUBLISH_QUEUE'] . '. Не предусмотрено ТЗ'
                        );
                        (new Telegram())->send(
                            'RabbitBot', 'RabbitMessages',
                            'Не добавлено в очередь ' . $queueName['PUBLISH_QUEUE'] . '. Не предусмотрено ТЗ',
                            'RabbitInfo'
                        );
                    }
                }
            }
        } catch (\Exception $e) {
            // TODO elastic
            var_dump($e);
            (new Telegram())->send('RabbitBot', 'RabbitMessages', print_r($e, true), 'RabbitError');
            $isSuccess = false;
        }
        var_dump($isSuccess);
        return $isSuccess;
    }
}
