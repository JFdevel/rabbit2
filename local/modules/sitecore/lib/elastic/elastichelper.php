<?php

namespace SiteCore\Elastic;

use Logs\Log;
use SiteCore\Tools\Telegram;
use Exception;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Channel\AMQPChannel;
use Bitrix\Main\Config\Configuration;
use SiteCore\Tools\DataAlteration;

/**
 * Class ElasticHelper
 * @package SiteCore\Elastic
 */
class ElasticHelper
{
    private function sendRequest(string $indexName, array $fields, string $method, string $url = ''): string
    {
        $baseUrl = 'https://url:9200/' . $indexName . $url;

        $curlInit = curl_init();
        curl_setopt($curlInit, CURLOPT_URL, $baseUrl);
        curl_setopt($curlInit, CURLOPT_PORT, '9200');
        curl_setopt($curlInit, CURLOPT_TIMEOUT, 200);
        curl_setopt($curlInit, CURLOPT_RETURNTRANSFER, 1);
        curl_setopt($curlInit, CURLOPT_FORBID_REUSE, 0);
        curl_setopt($curlInit, CURLOPT_CUSTOMREQUEST, $method);
        if (!empty($fields)) {
            curl_setopt($curlInit, CURLOPT_POSTFIELDS, json_encode($fields));
        }
        curl_setopt($curlInit, CURLOPT_HTTPHEADER, array("Content-Type: application/json"));
        curl_setopt($curlInit, CURLOPT_USERPWD, "usr:passwd");

        $response = curl_exec($curlInit);

        if (curl_errno($curlInit)) {
            (new Telegram())->send(
                'RabbitBot',
                'RabbitMessages',
                'Elastic. ' . curl_error($curlInit), 'RabbitError'
            );
        }
        return $response;
    }

    public function getIndexName(): string
    {
        return 'rab_exchange_' . (DataAlteration::isDev() ? 'test' : 'prod') . '_' . date('y_m_d');
    }

    public function parseMessages(string $mess, string $direction, string $integrationStatus = "", $errorMessage = ""): array
    {
        $messages = json_decode($mess);
        foreach ($messages as $message) {
            $logData = [
                "Adapter" => $message->digest->recipientAdapters,
                "ErrorMessage" => $errorMessage,
                "IntegrationDirection" => $direction,
                "IntegrationId" => $message->digest->integrationId,
                "IntegrationStatus" => $integrationStatus,
                "Message" => $message->xml,
                "SessionId" => md5($message->digest->integrationId . $message->xml),
                "StackTrace" => json_encode(
                    debug_backtrace(DEBUG_BACKTRACE_IGNORE_ARGS),
                    JSON_UNESCAPED_UNICODE
                ),
                "System" => "WEB_" . (DataAlteration::isDev() ? 'Dev' : 'Prod'),
            ];
            $this->writeMessage($logData);
        }

        return $messages;
    }

    public function writeMessage(array $fields): bool
    {
        $this->sendRequest($this->getIndexName(), $fields, 'POST', '/_doc');
        $ch = curl_init();

        curl_exec($ch);

        return true;
    }

    public function createIndex(string $indexName): bool
    {
        $struct = self::getDefaultStruct();
        $response = self::sendRequest($indexName, [], 'PUT');

        $messages = json_decode($response, true);
        if ($messages['error']) {
            (new Telegram())->send(
                'RabbitBot', 'RabbitTest',
                'Ошибка при создании индекса в эластике: ' . $messages['error']['reason'],
                ''
            );
            return false;
        }

        $response = self::sendRequest($indexName, $struct, 'PUT', '/_mapping');

        $messages = json_decode($response, true);
        if ($messages['error']) {
            (new Telegram())->send(
                'RabbitBot', 'RabbitTest',
                'Ошибка при обновлении маппинга в эластике: ' . $messages['error']['reason'],
                ''
            );
            return false;
        }

        return true;
    }

    private function getDefaultStruct(): array
    {
        return [
            "properties" => [
                'Message' => [
                    'type' => 'text',
                ],
                'StackTrace' => [
                    'type' => 'text',
                ],
                'IntegrationStatus' => [
                    'type' => 'keyword',
                ],
                'Adapter' => [
                    'type' => 'keyword',
                ],
                'IntegrationId' => [
                    'type' => 'text',
                ],
                'IntegrationDirection' => [
                    'type' => 'keyword',
                ],
                'CreatedOn' => [
                    'type' => 'date'
                ],
                'ErrorMessage' => [
                    'type' => 'text',
                ],
                'SessionId' => [
                    'type' => 'text',
                ],
                'System' => [
                    'type' => 'keyword',
                ],
            ],
        ];
    }

    public function elasticLog(
        string $mess, string $queueName, string $routeKey,
        string $direction, $integrationStatus = "", $errorMessage = ""
    ): array
    {
        $messages = json_decode($mess);

        foreach ($messages as $message) {
            $logData = [
                "Adapter" => implode(', ', $message->digest->recipientAdapters),
                "ErrorMessage" => $errorMessage,
                "QueueName" => $queueName,
                "RouteKey" => $routeKey,
                "CreatedOn" => date("Y-m-d\TH:i:s"),
                "IntegrationDirection" => $direction,
                "IntegrationId" => $message->digest->integrationId,
                "IntegrationStatus" => $integrationStatus,
                "Message" => $message->xml,
                "SessionId" => md5($message->digest->integrationId . $message->xml),
                "StackTrace" => json_encode(
                    debug_backtrace(DEBUG_BACKTRACE_IGNORE_ARGS),
                    JSON_UNESCAPED_UNICODE
                ),
                "System" => "WEB_" . (DataAlteration::isDev() ? 'Dev' : 'Prod'),
            ];
            $this->sendRequest($this->getIndexName(), $logData, 'POST', '/_doc');
        }

        return $messages;
    }
}