<?php

use Pixie\Connection;
use PDO;
use Pixie\Exception;

beforeEach(function () {
    // Set up SQLite in-memory connection
    $config = [
        'driver' => 'sqlite',
        'database' => ':memory:',
        'prefix' => 'cb_',
    ];

    $connection = new Connection('sqlite', $config);
    $this->builder = $connection->getQueryBuilder();

    $pdo = $connection->getPdoInstance();

    // Create tables with prefix 'cb_'
    $pdo->exec("
        CREATE TABLE cb_my_table (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT
        );
    ");

    $pdo->exec("
        CREATE TABLE cb_test (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT
        );
    ");

    // Insert sample data for raw query test
    $stmt = $pdo->prepare("INSERT INTO cb_my_table (id, name) VALUES (?, ?)");
    $stmt->execute([5, 'usman']);
});

it('can run a raw query with bindings', function () {
    $query = 'select * from cb_my_table where id = ? and name = ?';
    $bindings = [5, 'usman'];

    $result = $this->builder->query($query, $bindings)->get();

    expect($result)->toHaveCount(1);
    expect($result[0])->toBeInstanceOf(stdClass::class);
    expect($result[0]->id)->toBe(5);
    expect($result[0]->name)->toBe('usman');
});

it('insert query returns id for insert', function () {
    $id = $this->builder->table('test')->insert([
        'id' => 5,
        'name' => 'usman',
    ]);

    expect((int) $id)->toBeInt()->toBeGreaterThan(0);

    // Verify the inserted row
    $stmt = $this->builder->getConnection()->getPdoInstance()->prepare('SELECT * FROM cb_test WHERE id = ?');
    $stmt->execute([5]);
    $row = $stmt->fetch(PDO::FETCH_ASSOC);

    expect($row)->not->toBeFalse();
    expect($row['name'])->toBe('usman');
});

it('throws exception when table not specified', function () {
    $this->builder->where('a', 'b')->get();
})->throws(Exception::class, 'No table specified.', 3);
