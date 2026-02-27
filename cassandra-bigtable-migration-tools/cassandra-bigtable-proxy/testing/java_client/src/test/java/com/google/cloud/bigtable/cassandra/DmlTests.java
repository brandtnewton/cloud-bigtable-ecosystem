package com.google.cloud.bigtable.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class DmlTests {

  private static CqlSession session;
  private static final String TABLE = "orders";

  @BeforeAll
  public static void setup() {
    session = Utils.createClient("bigtabledevinstance");
    session.execute("CREATE TABLE IF NOT EXISTS " + TABLE + " (user_id text, order_num int, name varchar, created_at timestamp, PRIMARY KEY (user_id, order_num))");
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
    session.execute("INSERT INTO " + TABLE + " (user_id, order_num, name) VALUES ('literal123', 1, 'literal bob')");

    // Select
    ResultSet rs = session.execute("SELECT * FROM " + TABLE + " WHERE user_id='literal123' AND order_num=1");
    Row row = rs.one();
    assertNotNull(row, "Row should not be null");
    assertEquals("literal123", row.getString("user_id"));
    assertEquals(1, row.getInt("order_num"));
    assertEquals("literal bob", row.getString("name"));

    // Update
    session.execute("UPDATE " + TABLE + " SET name='literal bob updated' WHERE user_id='literal123' AND order_num=1");
    rs = session.execute("SELECT * FROM " + TABLE + " WHERE user_id='literal123' AND order_num=1");
    row = rs.one();
    assertNotNull(row, "Row should not be null after update");
    assertEquals("literal bob updated", row.getString("name"));

    // Delete
    session.execute("DELETE FROM " + TABLE + " WHERE user_id='literal123' AND order_num=1");
    rs = session.execute("SELECT * FROM " + TABLE + " WHERE user_id='literal123' AND order_num=1");
    row = rs.one();
    assertNull(row, "Row should be null after delete");
  }

  @Test
  public void testCrudNamedMarkers() {
    String userId = "named123";
    int orderNum = 2;
    String name = "named bob";

    // Insert
    PreparedStatement psInsert = session.prepare("INSERT INTO " + TABLE + " (user_id, order_num, name) VALUES (:u, :o, :n)");
    BoundStatement bsInsert = psInsert.bind().setString("u", userId).setInt("o", orderNum).setString("n", name);
    session.execute(bsInsert);

    // Select
    PreparedStatement psSelect = session.prepare("SELECT * FROM " + TABLE + " WHERE user_id=:u AND order_num=:o");
    BoundStatement bsSelect = psSelect.bind().setString("u", userId).setInt("o", orderNum);
    ResultSet rs = session.execute(bsSelect);
    Row row = rs.one();
    assertNotNull(row, "Row should not be null");
    assertEquals(userId, row.getString("user_id"));
    assertEquals(orderNum, row.getInt("order_num"));
    assertEquals(name, row.getString("name"));

    // Update
    PreparedStatement psUpdate = session.prepare("UPDATE " + TABLE + " SET name=:n WHERE user_id=:u AND order_num=:o");
    BoundStatement bsUpdate = psUpdate.bind().setString("n", name + " updated").setString("u", userId).setInt("o", orderNum);
    session.execute(bsUpdate);

    rs = session.execute(bsSelect);
    row = rs.one();
    assertNotNull(row, "Row should not be null after update");
    assertEquals(name + " updated", row.getString("name"));

    // Delete
    PreparedStatement psDelete = session.prepare("DELETE FROM " + TABLE + " WHERE user_id=:u AND order_num=:o");
    BoundStatement bsDelete = psDelete.bind().setString("u", userId).setInt("o", orderNum);
    session.execute(bsDelete);

    rs = session.execute(bsSelect);
    row = rs.one();
    assertNull(row, "Row should be null after delete");
  }

  @Test
  public void testCrudFailsWithBothPositionalAndNamedValues() {
    assertThrows(Exception.class, () -> {
      session.prepare("INSERT INTO " + TABLE + " (user_id, order_num, name) VALUES (:u, ?, :n)");
    });
  }

  @Test
  public void testCrudPositionalMarkers() {
    String userId = "pos123";
    int orderNum = 3;
    String name = "pos bob";

    // Insert
    PreparedStatement psInsert = session.prepare("INSERT INTO " + TABLE + " (user_id, order_num, name) VALUES (?, ?, ?)");
    session.execute(psInsert.bind(userId, orderNum, name));

    // Select
    PreparedStatement psSelect = session.prepare("SELECT * FROM " + TABLE + " WHERE user_id=? AND order_num=?");
    ResultSet rs = session.execute(psSelect.bind(userId, orderNum));
    Row row = rs.one();
    assertNotNull(row, "Row should not be null");
    assertEquals(userId, row.getString("user_id"));
    assertEquals(orderNum, row.getInt("order_num"));
    assertEquals(name, row.getString("name"));

    // Update
    PreparedStatement psUpdate = session.prepare("UPDATE " + TABLE + " SET name=? WHERE user_id=? AND order_num=?");
    session.execute(psUpdate.bind(name + " updated", userId, orderNum));

    rs = session.execute(psSelect.bind(userId, orderNum));
    row = rs.one();
    assertNotNull(row, "Row should not be null after update");
    assertEquals(name + " updated", row.getString("name"));

    // Delete
    PreparedStatement psDelete = session.prepare("DELETE FROM " + TABLE + " WHERE user_id=? AND order_num=?");
    session.execute(psDelete.bind(userId, orderNum));

    rs = session.execute(psSelect.bind(userId, orderNum));
    row = rs.one();
    assertNull(row, "Row should be null after delete");
  }

  @Test
  public void testInsertIfNotExists() {
    String userId = "lwt123";
    int orderNum = 4;
    String name = "lwt bob";

    // ensure the row does not exist.
    session.execute("DELETE FROM " + TABLE + " WHERE user_id='" + userId + "' AND order_num=" + orderNum);

    // Insert IF NOT EXISTS (should succeed)
    ResultSet rs = session.execute("INSERT INTO " + TABLE + " (user_id, order_num, name) VALUES ('" + userId + "', " + orderNum + ", '" + name + "') IF NOT EXISTS");
    assertTrue(rs.wasApplied(), "Insert should be applied");

    // Insert IF NOT EXISTS again (should fail)
    rs = session.execute("INSERT INTO " + TABLE + " (user_id, order_num, name) VALUES ('" + userId + "', " + orderNum + ", 'new name') IF NOT EXISTS");
    assertFalse(rs.wasApplied(), "Insert should not be applied second time");

    // Verify original data remains
    rs = session.execute("SELECT * FROM " + TABLE + " WHERE user_id='" + userId + "' AND order_num=" + orderNum);
    Row row = rs.one();
    assertNotNull(row);
    assertEquals(name, row.getString("name"));

    // Cleanup
    session.execute("DELETE FROM " + TABLE + " WHERE user_id='" + userId + "' AND order_num=" + orderNum);
  }

  @Test
  public void testCrudWithTimestamp() {
    String userId = "ts123";
    int orderNum = 6;
    String tsLiteral = "2023-10-27T10:00:00Z";

    // Insert with timestamp literal
    session.execute("INSERT INTO " + TABLE + " (user_id, order_num, name, created_at) VALUES ('" + userId + "', " + orderNum + ", 'ts bob', '" + tsLiteral + "')");

    // Select
    ResultSet rs = session.execute("SELECT * FROM " + TABLE + " WHERE user_id='" + userId + "' AND order_num=" + orderNum);
    Row row = rs.one();
    assertNotNull(row);
    assertNotNull(row.getInstant("created_at"));

    // Update with toTimestamp(now())
    session.execute("UPDATE " + TABLE + " SET created_at=toTimestamp(now()) WHERE user_id='" + userId + "' AND order_num=" + orderNum);
    rs = session.execute("SELECT * FROM " + TABLE + " WHERE user_id='" + userId + "' AND order_num=" + orderNum);
    row = rs.one();
    assertNotNull(row);
    assertNotNull(row.getInstant("created_at"));

    // Cleanup
    session.execute("DELETE FROM " + TABLE + " WHERE user_id='" + userId + "' AND order_num=" + orderNum);
  }

  @Test
  public void testInsertAndUpdateWithNull() {
    String userId = "null123";
    int orderNum = 5;

    // Insert with null literal
    session.execute("INSERT INTO " + TABLE + " (user_id, order_num, name, created_at) VALUES ('" + userId + "', " + orderNum + ", null, null)");
    ResultSet rs = session.execute("SELECT * FROM " + TABLE + " WHERE user_id='" + userId + "' AND order_num=" + orderNum);
    Row row = rs.one();
    assertNotNull(row, "Row should not be null after insert with null literal");
    assertNull(row.getString("name"), "Name should be null");
    assertNull(row.getInstant("created_at"), "created_at should be null");

    // Update with null literal
    session.execute("UPDATE " + TABLE + " SET name='some name', created_at=toTimestamp(now()) WHERE user_id='" + userId + "' AND order_num=" + orderNum);
    session.execute("UPDATE " + TABLE + " SET name=null, created_at=null WHERE user_id='" + userId + "' AND order_num=" + orderNum);
    rs = session.execute("SELECT * FROM " + TABLE + " WHERE user_id='" + userId + "' AND order_num=" + orderNum);
    row = rs.one();
    assertNotNull(row, "Row should not be null after update to null literal");
    assertNull(row.getString("name"), "Name should be null after update to null literal");
    assertNull(row.getInstant("created_at"), "created_at should be null after update to null literal");

    // Insert with null prepared statement (positional)
    userId = "null456";
    PreparedStatement psInsert = session.prepare("INSERT INTO " + TABLE + " (user_id, order_num, name, created_at) VALUES (?, ?, ?, ?)");
    session.execute(psInsert.bind(userId, orderNum, null, null));
    rs = session.execute("SELECT * FROM " + TABLE + " WHERE user_id='" + userId + "' AND order_num=" + orderNum);
    row = rs.one();
    assertNotNull(row, "Row should not be null after insert with null parameter");
    assertNull(row.getString("name"), "Name should be null");
    assertNull(row.getInstant("created_at"), "created_at should be null");

    // Update with null prepared statement (positional)
    PreparedStatement psUpdate = session.prepare("UPDATE " + TABLE + " SET name=?, created_at=? WHERE user_id=? AND order_num=?");
    session.execute(psUpdate.bind(null, null, userId, orderNum));
    rs = session.execute("SELECT * FROM " + TABLE + " WHERE user_id='" + userId + "' AND order_num=" + orderNum);
    row = rs.one();
    assertNotNull(row, "Row should not be null after update to null parameter");
    assertNull(row.getString("name"), "Name should be null after update to null parameter");
    assertNull(row.getInstant("created_at"), "created_at should be null after update to null parameter");

    // Insert with null prepared statement (named)
    userId = "null789";
    PreparedStatement psInsertNamed = session.prepare("INSERT INTO " + TABLE + " (user_id, order_num, name, created_at) VALUES (:u, :o, :n, :c)");
    session.execute(psInsertNamed.bind().setString("u", userId).setInt("o", orderNum).setToNull("n").setToNull("c"));
    rs = session.execute("SELECT * FROM " + TABLE + " WHERE user_id='" + userId + "' AND order_num=" + orderNum);
    row = rs.one();
    assertNotNull(row, "Row should not be null after insert with named null parameter");
    assertNull(row.getString("name"), "Name should be null after insert with named null parameter");
    assertNull(row.getInstant("created_at"), "created_at should be null after insert with named null parameter");

    // Update with null prepared statement (named)
    PreparedStatement psUpdateNamed = session.prepare("UPDATE " + TABLE + " SET name=:n, created_at=:c WHERE user_id=:u AND order_num=:o");
    session.execute(psUpdateNamed.bind().setToNull("n").setToNull("c").setString("u", userId).setInt("o", orderNum));
    rs = session.execute("SELECT * FROM " + TABLE + " WHERE user_id='" + userId + "' AND order_num=" + orderNum);
    row = rs.one();
    assertNotNull(row, "Row should not be null after update to named null parameter");
    assertNull(row.getString("name"), "Name should be null after update to named null parameter");
    assertNull(row.getInstant("created_at"), "created_at should be null after update to named null parameter");

    // Cleanup
    session.execute("DELETE FROM " + TABLE + " WHERE user_id='null123' AND order_num=5");
    session.execute("DELETE FROM " + TABLE + " WHERE user_id='null456' AND order_num=5");
    session.execute("DELETE FROM " + TABLE + " WHERE user_id='null789' AND order_num=5");
  }
}
