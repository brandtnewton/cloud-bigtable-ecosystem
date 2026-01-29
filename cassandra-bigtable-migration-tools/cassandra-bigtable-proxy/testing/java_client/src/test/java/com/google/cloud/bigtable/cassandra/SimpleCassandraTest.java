package com.google.cloud.bigtable.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class SimpleCassandraTest {

  private static CqlSession session;
  private static final String KEYSPACE = "bigtabledevinstance";
  private static final String TABLE = "orders";

  @BeforeAll
  public static void setup() {
    session = CqlSession.builder()
        .addContactPoint(new InetSocketAddress("127.0.0.1", 9042))
        .withLocalDatacenter("datacenter1")
        .build();

    session.execute("CREATE TABLE IF NOT EXISTS " + KEYSPACE + "." + TABLE + " (user_id text, order_num int, name varchar, PRIMARY KEY (user_id, order_num))");
  }

  @AfterAll
  public static void teardown() {
    if (session != null) {
      session.close();
    }
  }

  @Test
  public void testCrudLiteral() {
    // Insert
    session.execute("INSERT INTO " + KEYSPACE + "." + TABLE + " (user_id, order_num, name) VALUES ('literal123', 1, 'literal bob')");

    // Select
    ResultSet rs = session.execute("SELECT * FROM " + KEYSPACE + "." + TABLE + " WHERE user_id='literal123' AND order_num=1");
    Row row = rs.one();
    assertNotNull(row, "Row should not be null");
    assertEquals("literal123", row.getString("user_id"));
    assertEquals(1, row.getInt("order_num"));
    assertEquals("literal bob", row.getString("name"));

    // Update
    session.execute("UPDATE " + KEYSPACE + "." + TABLE + " SET name='literal bob updated' WHERE user_id='literal123' AND order_num=1");
    rs = session.execute("SELECT * FROM " + KEYSPACE + "." + TABLE + " WHERE user_id='literal123' AND order_num=1");
    row = rs.one();
    assertNotNull(row, "Row should not be null after update");
    assertEquals("literal bob updated", row.getString("name"));

    // Delete
    session.execute("DELETE FROM " + KEYSPACE + "." + TABLE + " WHERE user_id='literal123' AND order_num=1");
    rs = session.execute("SELECT * FROM " + KEYSPACE + "." + TABLE + " WHERE user_id='literal123' AND order_num=1");
    row = rs.one();
    assertNull(row, "Row should be null after delete");
  }

  @Test
  public void testCrudNamedMarkers() {
    String userId = "named123";
    int orderNum = 2;
    String name = "named bob";

    // Insert
    PreparedStatement psInsert = session.prepare("INSERT INTO " + KEYSPACE + "." + TABLE + " (user_id, order_num, name) VALUES (:u, :o, :n)");
    BoundStatement bsInsert = psInsert.bind().setString("u", userId).setInt("o", orderNum).setString("n", name);
    session.execute(bsInsert);

    // Select
    PreparedStatement psSelect = session.prepare("SELECT * FROM " + KEYSPACE + "." + TABLE + " WHERE user_id=:u AND order_num=:o");
    BoundStatement bsSelect = psSelect.bind().setString("u", userId).setInt("o", orderNum);
    ResultSet rs = session.execute(bsSelect);
    Row row = rs.one();
    assertNotNull(row, "Row should not be null");
    assertEquals(userId, row.getString("user_id"));
    assertEquals(orderNum, row.getInt("order_num"));
    assertEquals(name, row.getString("name"));

    // Update
    PreparedStatement psUpdate = session.prepare("UPDATE " + KEYSPACE + "." + TABLE + " SET name=:n WHERE user_id=:u AND order_num=:o");
    BoundStatement bsUpdate = psUpdate.bind().setString("n", name + " updated").setString("u", userId).setInt("o", orderNum);
    session.execute(bsUpdate);

    rs = session.execute(bsSelect);
    row = rs.one();
    assertNotNull(row, "Row should not be null after update");
    assertEquals(name + " updated", row.getString("name"));

    // Delete
    PreparedStatement psDelete = session.prepare("DELETE FROM " + KEYSPACE + "." + TABLE + " WHERE user_id=:u AND order_num=:o");
    BoundStatement bsDelete = psDelete.bind().setString("u", userId).setInt("o", orderNum);
    session.execute(bsDelete);

    rs = session.execute(bsSelect);
    row = rs.one();
    assertNull(row, "Row should be null after delete");
  }

  @Test
  public void testCrudPositionalMarkers() {
    String userId = "pos123";
    int orderNum = 3;
    String name = "pos bob";

    // Insert
    PreparedStatement psInsert = session.prepare("INSERT INTO " + KEYSPACE + "." + TABLE + " (user_id, order_num, name) VALUES (?, ?, ?)");
    session.execute(psInsert.bind(userId, orderNum, name));

    // Select
    PreparedStatement psSelect = session.prepare("SELECT * FROM " + KEYSPACE + "." + TABLE + " WHERE user_id=? AND order_num=?");
    ResultSet rs = session.execute(psSelect.bind(userId, orderNum));
    Row row = rs.one();
    assertNotNull(row, "Row should not be null");
    assertEquals(userId, row.getString("user_id"));
    assertEquals(orderNum, row.getInt("order_num"));
    assertEquals(name, row.getString("name"));

    // Update
    PreparedStatement psUpdate = session.prepare("UPDATE " + KEYSPACE + "." + TABLE + " SET name=? WHERE user_id=? AND order_num=?");
    session.execute(psUpdate.bind(name + " updated", userId, orderNum));

    rs = session.execute(psSelect.bind(userId, orderNum));
    row = rs.one();
    assertNotNull(row, "Row should not be null after update");
    assertEquals(name + " updated", row.getString("name"));

    // Delete
    PreparedStatement psDelete = session.prepare("DELETE FROM " + KEYSPACE + "." + TABLE + " WHERE user_id=? AND order_num=?");
    session.execute(psDelete.bind(userId, orderNum));

    rs = session.execute(psSelect.bind(userId, orderNum));
    row = rs.one();
    assertNull(row, "Row should be null after delete");
  }
}
