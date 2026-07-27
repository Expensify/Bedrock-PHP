<?php

declare(strict_types=1);

namespace Expensify\Bedrock\Tests;

use Expensify\Bedrock\LocalDB;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

/**
 * Tests for LocalDB::readAll(), the multi-row read added for per-type GROUP BY queries.
 */
final class LocalDBReadAllTest extends TestCase
{
    private string $path;
    private LocalDB $db;

    protected function setUp(): void
    {
        $this->path = tempnam(sys_get_temp_dir(), 'localdb-test-');
        $this->db = new LocalDB($this->path, new NullLogger(), new \stdClass());
        $this->db->open();
        $this->db->write('CREATE TABLE t (name TEXT, n INTEGER);');
        $this->db->write("INSERT INTO t (name, n) VALUES ('a', 1), ('a', 2), ('b', 5);");
    }

    protected function tearDown(): void
    {
        $this->db->close();
        @unlink($this->path);
    }

    public function testReadAllReturnsEveryRow(): void
    {
        // When we run a grouped query that yields multiple rows
        $rows = $this->db->readAll('SELECT name, SUM(n) FROM t GROUP BY name ORDER BY name;');

        // Then every row is returned as a numeric array
        $this->assertSame([['a', 3], ['b', 5]], $rows);
    }

    public function testReadStillReturnsOnlyTheFirstRow(): void
    {
        // Given the same multi-row query, the single-row read() returns just the first row
        $row = $this->db->read('SELECT name, SUM(n) FROM t GROUP BY name ORDER BY name;');

        $this->assertSame(['a', 3], $row);
    }

    public function testReadAllReturnsEmptyArrayWhenNoRows(): void
    {
        // When a query matches nothing
        $rows = $this->db->readAll("SELECT name, n FROM t WHERE name = 'nope';");

        // Then we get an empty array, not false
        $this->assertSame([], $rows);
    }
}
