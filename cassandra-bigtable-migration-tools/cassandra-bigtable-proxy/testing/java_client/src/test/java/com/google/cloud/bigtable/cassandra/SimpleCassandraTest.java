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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

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

  }

  @AfterAll
  public static void teardown() {
    if (session != null) {
      session.close();
    }
  }

  @Test
  public void testInsertAndSelect() {
    // Insert
    PreparedStatement prepared = session.prepare("INSERT INTO " + KEYSPACE + "." + TABLE + " (user_id, order_num, name) VALUES (:id, :num, :name)");

    BoundStatement bound = prepared.bind()
        .setString("id", "u123")
        .setInt("num", 3)
        .setString("name", "Doe");

    session.execute(bound);

    // Select
    ResultSet rs = session.execute("SELECT * FROM " + KEYSPACE + "." + TABLE + " WHERE user_id='u123'");
    Row row = rs.one();

    assertNotNull(row, "Row should not be null");
    assertEquals("u123", row.getString("user_id"));
    assertEquals("Doe", row.getString("name"));
  }
}
