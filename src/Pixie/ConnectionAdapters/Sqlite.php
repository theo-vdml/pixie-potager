<?php
namespace Pixie\ConnectionAdapters;

class Sqlite extends BaseAdapter
{
    /**
     * @param $config
     *
     * @return mixed
     */
    public function doConnect($config)
    {
        $connectionString = 'sqlite:' . $config['database'];
        return new \PDO($connectionString, null, null, $config['options']);
    }
}
