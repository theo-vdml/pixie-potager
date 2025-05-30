<?php
namespace Pixie;

use Pixie\QueryBuilder\QueryBuilderHandler;

class Connection
{
    /**
     * @var string
     */
    protected $adapter;

    /**
     * @var array
     */
    protected $adapterConfig;

    /**
     * @var \PDO
     */
    protected $pdoInstance;

    /**
     * @var Connection
     */
    protected static $storedConnection;

    /**
     * @var EventHandler
     */
    protected $eventHandler;

    /**
     * @param               $adapter
     * @param array         $adapterConfig
     * @param null|string   $alias
     */
    public function __construct($adapter, array $adapterConfig, $alias = null)
    {
        $this->setAdapter($adapter)->setAdapterConfig($adapterConfig)->connect();

        // Create event dependency
        $this->eventHandler = new EventHandler();

        if ($alias) {
            $this->createAlias($alias);
        }
    }

    /**
     * Create an easily accessible query builder alias
     *
     * @param $alias
     */
    public function createAlias($alias)
    {
        class_alias('Pixie\\AliasFacade', $alias);
        $builder = new QueryBuilderHandler($this);
        AliasFacade::setQueryBuilderInstance($builder);
    }

    /**
     * Returns an instance of Query Builder
     */
    public function getQueryBuilder()
    {
        return new QueryBuilderHandler($this);
    }


    /**
     * Create the connection adapter
     */
    protected function connect()
    {
        // Build a database connection if we don't have one connected

        $adapterClass = '\\Pixie\\ConnectionAdapters\\' . ucfirst(strtolower($this->adapter));

        /** @var \Pixie\ConnectionAdapters\BaseAdapter $adapterInstance */
        $adapterInstance = new $adapterClass();
        $pdo = $adapterInstance->connect($this->adapterConfig);

        $this->setPdoInstance($pdo);

        // Preserve the first database connection with a static property
        if (!static::$storedConnection) {
            static::$storedConnection = $this;
        }
    }

    /**
     * @param \PDO $pdo
     *
     * @return $this
     */
    public function setPdoInstance(\PDO $pdo)
    {
        $this->pdoInstance = $pdo;
        return $this;
    }

    /**
     * @return \PDO
     */
    public function getPdoInstance()
    {
        return $this->pdoInstance;
    }

    /**
     * @param $adapter
     *
     * @return $this
     */
    public function setAdapter($adapter)
    {
        $this->adapter = $adapter;
        return $this;
    }

    /**
     * @return string
     */
    public function getAdapter()
    {
        return $this->adapter;
    }

    /**
     * @param array $adapterConfig
     *
     * @return $this
     */
    public function setAdapterConfig(array $adapterConfig)
    {
        $this->adapterConfig = $adapterConfig;
        return $this;
    }

    /**
     * @return array
     */
    public function getAdapterConfig(): array
    {
        return $this->adapterConfig;
    }

    /**
     * @return EventHandler
     */
    public function getEventHandler()
    {
        return $this->eventHandler;
    }

    /**
     * @return Connection
     */
    public static function getStoredConnection()
    {
        return static::$storedConnection;
    }
}
