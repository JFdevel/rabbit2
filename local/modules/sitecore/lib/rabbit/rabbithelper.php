<?php

namespace SiteCore\Rabbit;

use Logs\Log;
use Exception;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Channel\AMQPChannel;
use Bitrix\Main\Config\Configuration;

/**
 * Class RabbitHelper
 * @package SiteCore\Rabbit
 */
class RabbitHelper
{
    public AMQPStreamConnection $connection;
    public AMQPChannel $channel;
    public Log $logger;
    /**
     * @var array Список всех доступных очередей
     *
     * array_key - Наименование очереди
     * LISTEN_ROUTE - Ключи маршрутириации
     * PUBLISH_QUEUE - Наименование очереди для публикации сообщений
     * PUBLISH_ROUTE - Роутинг для публикации сообщений
     */
    public const MESSAGE_LIMIT = 3;
    public const QUEUE_CONNECTOR = [
        'WEB_Contact' => [
            'LISTEN_ROUTE' => ['crm_contact', '1c_contact'],
            'PUBLISH_QUEUE' => ['CRM_Contact', '1C_Contact'],
            'PUBLISH_ROUTE' => 'web_contact'
        ],
        'WEB_Account' => [
            'LISTEN_ROUTE' => ['crm_account', '1c_account'],
            'PUBLISH_QUEUE' => ['CRM_Account', '1C_Account'],
            'PUBLISH_ROUTE' => 'web_account',
        ],
        'WEB_Voucher' => [
            'LISTEN_ROUTE' => ['crm_voucher', '1c_voucher'],
            'PUBLISH_QUEUE' => ['CRM_Voucher', '1C_Voucher'],
            'PUBLISH_ROUTE' => 'web_voucher',
        ],
        'WEB_Product' => [
            'LISTEN_ROUTE' => ['1c_product'],
            'PUBLISH_QUEUE' => [],
            'PUBLISH_ROUTE' => '',
        ],
        'WEB_Contract' => [
            'LISTEN_ROUTE' => ['crm_contract', '1c_contract'],
            'PUBLISH_QUEUE' => ['CRM_Contract', '1C_Contract'],
            'PUBLISH_ROUTE' => 'web_contract',
        ],
        'WEB_TradingPoint' => [
            'LISTEN_ROUTE' => ['1c_tradingpoint', 'crm_tradingpoint'],
            'PUBLISH_QUEUE' => ['CRM_TradingPoint', '1C_TradingPoint'],
            'PUBLISH_ROUTE' => 'web_tradingpoint',
        ],
        'WEB_Order' => [
            'LISTEN_ROUTE' => ['1c_order', 'crm_order'],
            'PUBLISH_QUEUE' => ['CRM_Order', '1C_Order'],
            'PUBLISH_ROUTE' => 'web_order',
        ],
        'WEB_BreedFeed' => [
            'LISTEN_ROUTE' => ['1c_breedfeed', 'crm_breedfeed'],
            'PUBLISH_QUEUE' => ['CRM_BreedFeed', '1C_BreedFeed'],
            'PUBLISH_ROUTE' => 'web_breedfeed',
        ],
        'WEB_AdditionalLoyaltyPoints' => [
            'LISTEN_ROUTE' => ['crm_additionalloyaltypoints'],
            'PUBLISH_QUEUE' => ['CRM_AdditionalLoyaltyPoints'],
            'PUBLISH_ROUTE' => 'web_additionalloyaltypoints',
        ],
        'WEB_FirstStepRequest' => [
            'LISTEN_ROUTE' => ['1c_firststeprequest', 'crm_firststeprequest'],
            'PUBLISH_QUEUE' => ['CRM_FirstStepRequest', '1C_FirstStepRequest'],
            'PUBLISH_ROUTE' => 'web_firststeprequest',
        ],
        'WEB_ProductPrice' => [
            'LISTEN_ROUTE' => ['1c_productprice'],
            'PUBLISH_QUEUE' => [],
            'PUBLISH_ROUTE' => '',
        ],
        'WEB_TradingPoint_Loyalty' => [
            'LISTEN_ROUTE' => ['1c_tradingpoint_loyalty'],
            'PUBLISH_QUEUE' => ['CRM_TradingPoint_Loyalty', '1C_TradingPoint_Loyalty'],
            'PUBLISH_ROUTE' => 'web_tradingpoint_loyalty',
        ],
    ];

    /**
     * @throws Exception
     */
    public function __construct()
    {
        $this->connect();
    }

    /**
     * @throws Exception
     */
    public function connect(): void
    {
        $settings = $this->getSettings();
        $this->connection = new AMQPStreamConnection(
            $settings['host'],
            $settings['port'],
            $settings['user'],
            $settings['password'],
            $settings['vhost'],
            false,
            'AMQPLAIN',
            null,
            'en_US',
            600,
            600
        );

        $this->channel = $this->connection->channel();
    }

    /**
     * Метод возвращает настройки для подключения, указанные в .settings.php к RabbitMQ
     * @return array [
     *     'host' => (string) hostname.
     *     'port' => (string) port.
     *     'user' => (string) user.
     *     'password' => (string) password.
     *     'vhost' => (string) vhost.
     * ]
     * @throws Exception
     */

    private function getSettings(): array
    {
        $config = Configuration::getInstance();
        $configConnections = $config->getValue("rabbitmq");

        if (empty($configConnections)) {
            throw new \Exception('не заданы настройки подключения к RabbitMQ');
        }

        return $configConnections;
    }

}