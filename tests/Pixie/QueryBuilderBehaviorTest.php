<?php

use Pixie\Connection;

beforeEach(function () {
    // Set up SQLite in-memory connection
    $config = [
        'driver' => 'sqlite',
        'database' => ':memory:',
        'prefix' => 'cb_',
    ];

    $connection = new Connection('sqlite', $config);
    $this->builder = $connection->getQueryBuilder();

    // Create the cb_some_table for testing
    $pdo = $connection->getPdoInstance();
    $pdo->exec('CREATE TABLE cb_some_table (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, status INTEGER)');

    // Create a dummy cb_my_table
    $pdo = $connection->getPdoInstance();
    $pdo->exec("CREATE TABLE cb_my_table (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, key TEXT, value TEXT, counter INTEGER)");

});

test('select flexibility', function () {
    $query = $this->builder
        ->select('foo')
        ->select(['bar', 'baz'])
        ->select('qux', 'lol', 'wut')
        ->from('t');
    expect($query->getQuery()->getRawSql())->toBe('SELECT "foo", "bar", "baz", "qux", "lol", "wut" FROM "cb_t"');
});

test('select query', function () {
    $subQuery = $this->builder->table('person_details')->select('details')->where('person_id', '=', 3);

    $query = $this->builder->table('my_table')
        ->select('my_table.*')
        ->select(array($this->builder->raw('count(cb_my_table.id) as tot'), $this->builder->subQuery($subQuery, 'pop')))
        ->where('value', '=', 'Ifrah')
        ->whereNot('my_table.id', -1)
        ->orWhereNot('my_table.id', -2)
        ->orWhereIn('my_table.id', array(1, 2))
        ->groupBy(array('value', 'my_table.id', 'person_details.id'))
        ->orderBy('my_table.id', 'DESC')
        ->orderBy('value')
        ->having('tot', '<', 2)
        ->limit(1)
        ->offset(0)
        ->join(
            'person_details',
            'person_details.person_id',
            '=',
            'my_table.id'
        );

    $nestedQuery = $this->builder->table($this->builder->subQuery($query, 'bb'))->select('*');

    expect($nestedQuery->getQuery()->getRawSql())->toBe('SELECT * FROM (SELECT "cb_my_table".*, count(cb_my_table.id) as tot, (SELECT "details" FROM "cb_person_details" WHERE "person_id" = 3) as pop FROM "cb_my_table" INNER JOIN "cb_person_details" ON "cb_person_details"."person_id" = "cb_my_table"."id" WHERE "value" = \'Ifrah\' AND NOT "cb_my_table"."id" = -1 OR NOT "cb_my_table"."id" = -2 OR "cb_my_table"."id" IN (1, 2) GROUP BY "value", "cb_my_table"."id", "cb_person_details"."id" HAVING "tot" < 2 ORDER BY "cb_my_table"."id" DESC, "value" ASC LIMIT 1 OFFSET 0) as bb');
});

test('select aliases', function () {
    $query = $this->builder->from('my_table')->select('foo')->select(array('bar' => 'baz', 'qux'));

    expect($query->getQuery()->getRawSql())->toBe('SELECT "foo", "bar" AS "baz", "qux" FROM "cb_my_table"');
});

it('builds raw statements within where criteria', function () {
    $query = $this->builder->from('my_table')
        ->where('simple', 'criteria')
        ->where($this->builder->raw('RAW'))
        ->where($this->builder->raw('PARAMETERIZED_ONE(?)', 'foo'))
        ->where($this->builder->raw('PARAMETERIZED_SEVERAL(?, ?, ?)', [1, '2', 'foo']));

    expect($query->getQuery()->getRawSql())->toBe(
        'SELECT * FROM "cb_my_table" WHERE "simple" = \'criteria\' AND RAW AND PARAMETERIZED_ONE(\'foo\') AND PARAMETERIZED_SEVERAL(1, \'2\', \'foo\')'
    );
});

it('builds standalone whereNot clause', function () {
    $query = $this->builder->table('my_table')->whereNot('foo', 1);

    expect($query->getQuery()->getRawSql())->toBe(
        'SELECT * FROM "cb_my_table" WHERE NOT "foo" = 1'
    );
});

it('selects distinct with multiple columns', function () {
    $query = $this->builder->selectDistinct(['name', 'surname'])->from('my_table');

    expect($query->getQuery()->getRawSql())->toBe(
        'SELECT DISTINCT "name", "surname" FROM "cb_my_table"'
    );
});

it('selects distinct with a single column', function () {
    $query = $this->builder->selectDistinct('name')->from('my_table');

    expect($query->getQuery()->getRawSql())->toBe(
        'SELECT DISTINCT "name" FROM "cb_my_table"'
    );
});

it('handles selectDistinct and multiple select calls', function () {
    $query = $this->builder->select('name')
        ->selectDistinct('surname')
        ->select(['birthday', 'address'])
        ->from('my_table');

    expect($query->getQuery()->getRawSql())->toBe(
        'SELECT DISTINCT "name", "surname", "birthday", "address" FROM "cb_my_table"'
    );
});

it('builds a select query with nested criteria and joins', function () {
    $builder = $this->builder;

    $query = $builder->table('my_table')
        ->where('my_table.id', '>', 1)
        ->orWhere('my_table.id', 1)
        ->where(function ($q) {
            $q->where('value', 'LIKE', '%sana%');
            $q->orWhere(function ($q2) {
                $q2->where('key', 'LIKE', '%sana%');
                $q2->orWhere('value', 'LIKE', '%sana%');
            });
        })
        ->join(['person_details', 'a'], 'a.person_id', '=', 'my_table.id')
        ->leftJoin(['person_details', 'b'], function ($table) use ($builder) {
            $table->on('b.person_id', '=', 'my_table.id');
            $table->on('b.deleted', '=', $builder->raw(0));
            $table->orOn('b.age', '>', $builder->raw(1));
        });

    expect($query->getQuery()->getRawSql())->toBe(
        'SELECT * FROM "cb_my_table" ' .
        'INNER JOIN "cb_person_details" AS "cb_a" ON "cb_a"."person_id" = "cb_my_table"."id" ' .
        'LEFT JOIN "cb_person_details" AS "cb_b" ON "cb_b"."person_id" = "cb_my_table"."id" ' .
        'AND "cb_b"."deleted" = 0 OR "cb_b"."age" > 1 ' .
        'WHERE "cb_my_table"."id" > 1 OR "cb_my_table"."id" = 1 ' .
        'AND ("value" LIKE \'%sana%\' OR ("key" LIKE \'%sana%\' OR "value" LIKE \'%sana%\'))'
    );
});


it('applies a before-select event to modify the query', function () {
    $builder = $this->builder;

    $builder->registerEvent('before-select', ':any', function ($qb) {
        $qb->whereIn('status', [1, 2]);
    });

    $query = $builder->table('some_table')->where('name', 'Some');
    $query->get();

    expect($query->getQuery()->getRawSql())->toBe(
        'SELECT * FROM "cb_some_table" WHERE "name" = \'Some\' AND "status" IN (1, 2)'
    );
});


it('triggers before and after events for all query types', function () {
    $builder = $this->builder;
    $counter = 0;

    foreach (['before', 'after'] as $prefix) {
        foreach (['insert', 'select', 'update', 'delete'] as $action) {
            $builder->registerEvent("$prefix-$action", ':any', function ($qb) use (&$counter) {
                return $counter++;
            });
        }
    }

    $insert = $builder->table('foo')->insert(['bar' => 'baz']);
    expect($insert)->toBe(0); // 0 returned from before-insert
    expect($counter)->toBe(1); // after-insert

    $select = $builder->from('foo')->select('bar')->get();
    expect($select)->toBe(1); // return value of after-select
    expect($counter)->toBe(2);

    $update = $builder->table('foo')->update(['bar' => 'baz']);
    expect($update)->toBe(2);
    expect($counter)->toBe(3);

    $delete = $builder->from('foo')->delete();
    expect($delete)->toBe(3);
    expect($counter)->toBe(4);
});

it('builds an INSERT query', function () {
    $builder = $this->builder->from('my_table');
    $data = ['key' => 'Name', 'value' => 'Sana'];

    expect($builder->getQuery('insert', $data)->getRawSql())
        ->toBe('INSERT INTO "cb_my_table" ("key","value") VALUES (\'Name\',\'Sana\')');
});

it('builds an INSERT IGNORE query', function () {
    $builder = $this->builder->from('my_table');
    $data = ['key' => 'Name', 'value' => 'Sana'];

    expect($builder->getQuery('insertignore', $data)->getRawSql())
        ->toBe('INSERT IGNORE INTO "cb_my_table" ("key","value") VALUES (\'Name\',\'Sana\')');
});

it('builds a REPLACE query', function () {
    $builder = $this->builder->from('my_table');
    $data = ['key' => 'Name', 'value' => 'Sana'];

    expect($builder->getQuery('replace', $data)->getRawSql())
        ->toBe('REPLACE INTO "cb_my_table" ("key","value") VALUES (\'Name\',\'Sana\')');
});

it('builds an INSERT with ON DUPLICATE KEY UPDATE clause', function () {
    $insert = ['name' => 'Sana', 'counter' => 1];
    $update = ['name' => 'Sana', 'counter' => 2];

    $this->builder->from('my_table')->onDuplicateKeyUpdate($update);

    expect($this->builder->getQuery('insert', $insert)->getRawSql())
        ->toBe('INSERT INTO "cb_my_table" ("name","counter") VALUES (\'Sana\',1) ON DUPLICATE KEY UPDATE "name"=\'Sana\',"counter"=2');
});

it('builds an UPDATE query with a WHERE clause', function () {
    $builder = $this->builder->table('my_table')->where('value', 'Sana');
    $data = ['key' => 'Sana', 'value' => 'Amrin'];

    expect($builder->getQuery('update', $data)->getRawSql())
        ->toBe('UPDATE "cb_my_table" SET "key"=\'Sana\',"value"=\'Amrin\' WHERE "value" = \'Sana\'');
});

it('builds a DELETE query with a WHERE clause', function () {
    $builder = $this->builder->table('my_table')->where('value', '=', 'Amrin');

    expect($builder->getQuery('delete')->getRawSql())
        ->toBe('DELETE FROM "cb_my_table" WHERE "value" = \'Amrin\'');
});

it('allows flexible ORDER BY syntax', function () {
    $query = $this->builder
        ->from('t')
        ->orderBy('foo', 'DESC')
        ->orderBy(['bar', 'baz' => 'ASC', $this->builder->raw('raw1')], 'DESC')
        ->orderBy($this->builder->raw('raw2'), 'DESC');

    expect($query->getQuery()->getRawSql())
        ->toBe('SELECT * FROM "cb_t" ORDER BY "foo" DESC, "bar" DESC, "baz" ASC, raw1 DESC, raw2 DESC');
});

it('builds a query using NULL and NOT NULL conditions', function () {
    $query = $this->builder->from('my_table')
        ->whereNull('key1')
        ->orWhereNull('key2')
        ->whereNotNull('key3')
        ->orWhereNotNull('key4');

    expect($query->getQuery()->getRawSql())
        ->toBe('SELECT * FROM "cb_my_table" WHERE "key1" IS  NULL OR "key2" IS  NULL AND "key3" IS NOT NULL OR "key4" IS NOT NULL');
});

it('can use a subquery in a WHERE IN clause', function () {
    $sub = clone $this->builder;
    $query = $this->builder->from('my_table')->whereIn('foo', $this->builder->subQuery(
        $sub->from('some_table')->select('foo')->where('id', 1)
    ));

    expect($query->getQuery()->getRawSql())
        ->toBe('SELECT * FROM "cb_my_table" WHERE "foo" IN (SELECT "foo" FROM "cb_some_table" WHERE "id" = 1)');
});

it('can use a subquery in a WHERE NOT IN clause', function () {
    $sub = clone $this->builder;
    $query = $this->builder->from('my_table')->whereNotIn('foo', $this->builder->subQuery(
        $sub->from('some_table')->select('foo')->where('id', 1)
    ));

    expect($query->getQuery()->getRawSql())
        ->toBe('SELECT * FROM "cb_my_table" WHERE "foo" NOT IN (SELECT "foo" FROM "cb_some_table" WHERE "id" = 1)');
});

it('can set fetch mode via constructor', function () {
    $fetchMode = \PDO::FETCH_ASSOC;
    $builder = new \Pixie\QueryBuilder\QueryBuilderHandler($this->mockConnection, $fetchMode);

    expect($builder->getFetchMode())->toBe($fetchMode);
});

it('maintains fetch mode between builder instances', function () {
    $fetchMode = \PDO::FETCH_ASSOC;
    $builder = new \Pixie\QueryBuilder\QueryBuilderHandler($this->mockConnection, $fetchMode);
    $newBuilder = $builder->table('stuff');

    expect($newBuilder->getFetchMode())->toBe($fetchMode);
});
