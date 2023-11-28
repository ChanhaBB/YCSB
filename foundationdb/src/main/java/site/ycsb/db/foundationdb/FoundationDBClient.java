/**
 * Copyright (c) 2012 - 2016 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package site.ycsb.db.foundationdb;

import com.apple.foundationdb.*;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;
import site.ycsb.*;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static java.lang.System.getProperties;
import static java.util.stream.Collectors.toList;

/**
 * FoundationDB client for YCSB framework.
 */

public class FoundationDBClient extends DB {
  private static final String API_VERSION = "foundationdb.apiversion";
  private static final String API_VERSION_DEFAULT = "710";
  private static final String CLUSTER_FILE = "foundationdb.clusterfile";
  private static final String CLUSTER_FILE_DEFAULT = "./fdb.cluster";
  private static final String DB_NAME = "foundationdb.dbname";
  private static final String DB_NAME_DEFAULT = "DB";
  private static final String DB_BATCH_SIZE_DEFAULT = "0";
  private static final String DB_BATCH_SIZE = "foundationdb.batchsize";
  private static final String DATACENTER_ID = "foundationdb.datacenterid";

  private static final String DATACENTER_ID_DEFAULT = "";
  private static final String TLS_VERIFY_PEERS = "foundationdb.check_peers";
  private static final String TLS_VERIFY_PEERS_DEFAULT = "Check.Valid=0";

  private static final String TLS_CERTIFICATE_FILE = "foundationdb.certificate";
  private static final String TLS_CERTIFICATE_FILE_DEFAULT = "/etc/foundationdb/pki/client-server.crt";

  private static final String TLS_KEY_FILE = "foundationdb.key";
  private static final String TLS_KEY_FILE_DEFAULT = "/etc/foundationdb/pki/client-server.key";

  private static final byte[] TENANT_NAME = "adobe".getBytes();

  private static final String TLS_CA_FILE = "foundationdb.ca";
  private static final String TLS_CA_FILE_DEFAULT = "/etc/foundationdb/pki/ca.crt";
  private static Logger logger = LoggerFactory.getLogger(FoundationDBClient.class);
  private static FDB fdb;
  private static Database db;
  private static Tenant tenant;
  private static String dbName;
  private static int batchSize;
  private static volatile boolean isFDBInitialized = false;

  private List<Op> opList = new ArrayList<>();
  private enum OpName {READ, UPDATE, DELETE, INSERT}

  // initialize DB

  private static final class Op {
    private OpName opName;
    private String tableName;
    private String key;
    private Map<String, ByteIterator> values;

    public Op(OpName opName, String tableName, String rowKey, Map<String, ByteIterator> values) {
      this.opName = opName;
      this.tableName = tableName;
      this.key = rowKey;
      this.values = values;
    }
    public Op(OpName opName, String tableName, String rowKey) {
      this(opName, tableName, rowKey, new HashMap<>());
    }
  }

  // ----------------------  DB interface methods  --------------------
  @Override
  public Status read(String tableName, String key, Set<String> fields, Map<String, ByteIterator> result) {
    String rowKey = getRowKey(dbName, tableName, key);
    logger.debug("insert key = {}", rowKey);
    return performBatchOperation(new Op(OpName.READ, tableName, rowKey, result));
  }

  @Override
  public Status insert(String tableName, String key, Map<String, ByteIterator> values) {
    String rowKey = getRowKey(dbName, tableName, key);
    logger.debug("insert key = {}", rowKey);
    return performBatchOperation(new Op(OpName.INSERT, tableName, rowKey, values));
  }

  @Override
  public Status update(String tableName, String key, Map<String, ByteIterator> values) {
    String rowKey = getRowKey(dbName, tableName, key);
    logger.debug("update key = {}", rowKey);
    return performBatchOperation(new Op(OpName.UPDATE, tableName, rowKey, values));
  }

  @Override
  public Status delete(String tableName, String key) {
    String rowKey = getRowKey(dbName, tableName, key);
    logger.debug("delete key = {}", rowKey);
    return performBatchOperation(new Op(OpName.DELETE, tableName, rowKey));
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    String startRowKey = getRowKey(dbName, table, startkey);
    String endRowKey = getEndRowKey(table);
    logger.debug("scan key from {} to {} limit {} ", startkey, endRowKey, recordcount);
    try (Transaction tr = tenant.createTransaction()) {
      tr.options().setReadYourWritesDisable();
      AsyncIterable<KeyValue> entryList = tr.getRange(Tuple.from(startRowKey).pack(), Tuple.from(endRowKey).pack(),
          recordcount > 0 ? recordcount : 0);
      List<KeyValue> entries = entryList.asList().join();
      for (int i = 0; i < entries.size(); ++i) {
        final HashMap<String, ByteIterator> map = new HashMap<>();
        Tuple value = Tuple.fromBytes(entries.get(i).getValue());
        if (convTupleToMap(value, fields, map) == Status.OK) {
          result.add(map);
        } else {
          logger.error("Error scanning keys: from {} to {} limit {} ", startRowKey, endRowKey, recordcount);
          return Status.ERROR;
        }
      }
      return Status.OK;
    } catch (FDBException e) {
      logger.error(MessageFormatter.format("Error scanning keys: from {} to {} ",
          startRowKey, endRowKey).getMessage(), e);
      e.printStackTrace();
    } catch (Exception e) {
      logger.error(MessageFormatter.format("Error scanning keys: from {} to {} ",
          startRowKey, endRowKey).getMessage(), e);
      e.printStackTrace();
    }
    return Status.ERROR;
  }

  @Override
  public void cleanup() throws DBException {
    if (opList.size() > 0) {
      performBatchOperation(null, true);
    }
    try {
      tenant.close();
      db.close();
    } catch (FDBException e) {
      logger.error(MessageFormatter.format("Error in database operation: {}", "cleanup")
          .getMessage(), e);
      throw new DBException(e);
    }
  }

  // ----------------------  Core methods  --------------------

  private Status performBatchOperation(Op newOp, boolean runImmediately) {
    if (newOp != null) {
      opList.add(newOp);
    }

    if (!runImmediately && opList.size() < batchSize) {
      return Status.BATCHED_OK;
    }

    // group list items by op
    try {
      return tenant.run(tr -> {
          List<CompletableFuture<Status>> statusList = new ArrayList<>();
          opList.forEach(op -> {
              switch (op.opName) {
              case READ:
                statusList.add(performRead(tr, op));
                break;
              case UPDATE:
                statusList.add(performUpdate(tr, op));
                break;
              case INSERT:
                tr.set(Tuple.from(op.key).pack(), convertValueToTuple(op.values).pack());
                break;
              case DELETE:
                tr.clear(Tuple.from(op.key).pack());
                break;
              default:
                throw new UnsupportedOperationException("Unsupported OpName = " + op.opName);
              }
            });
          List<Status> statusList2 = sequence(statusList).join();
          boolean someFailure = statusList2.stream().anyMatch(s -> !s.isOk());
          statusList2.forEach(s -> System.err.print(s.getName() + " "));
          System.err.println();
          return someFailure ? Status.ERROR : Status.OK;
        });
    } catch (FDBException e) {
      String opListSerialized = opList.stream().map(o -> o.opName + ", " + o.key + ", " + o.values.toString() + ", " +
                                  o.tableName).collect(Collectors.joining("\n"));
      logger.error(MessageFormatter.format("FDB Error performing batch operation - ",
                    opListSerialized).getMessage(), e);
    } catch (Throwable e) {
      String opListSerialized = opList.stream().map(o -> o.opName + ", " + o.key + ", " + o.values.toString() + ", "
                                + o.tableName).collect(Collectors.joining("\n"));
      logger.error(MessageFormatter.format("Throwable Error performing batch operation - ",
                    opListSerialized).getMessage(), e);
    } finally {
      opList.clear();
      //tenant.close();
    }

    return Status.ERROR;
  }

  private Status performBatchOperation(Op newOp) {
    return performBatchOperation(newOp, false);
  }


  public static CompletableFuture<Status> performUpdate(Transaction tr, Op op) {
    return tr.get(Tuple.from(op.key).pack()).thenApply(row -> {
        if (row == null) {
          logger.debug("key not fount: {}", op.key);
          return Status.NOT_FOUND;
        }
        Tuple o = Tuple.fromBytes(row);
        if (o.size() == 0) {
          logger.debug("key not fount: {}", op.key);
          return Status.NOT_FOUND;
        }

        // Only include fields that were requested
        HashMap<String, ByteIterator> result = new HashMap<>();
        if (convTupleToMap(o, null, result) != Status.OK) {
          return Status.ERROR;
        }
        for (String k : op.values.keySet()) {
          result.put(k, op.values.get(k));
        }

        tr.set(Tuple.from(op.key).pack(), convertValueToTuple(result).pack());
        return Status.OK;
      });
  }

  private CompletableFuture<Status> performRead(Transaction tr, Op op) {
    return tr.get(Tuple.from(op.key).pack()).thenApply(row -> {
        if (row != null) {
          Tuple t = Tuple.fromBytes(row);
          if (t.size() > 0) {
            return convTupleToMap(t, op.values.keySet(), new HashMap<>());
          }
        }
        return Status.NOT_FOUND;
      });
  }


  // ----------------------  Helper methods  --------------------
  private static String getRowKey(String database, String table, String key) {
    return database + ":" + table + ":" + key;
  }

  private static String getEndRowKey(String table) {
    return table + ";";
  }

  private static Tuple convertValueToTuple(Map<String, ByteIterator> values) {
    Tuple t = new Tuple();
    for (Map.Entry<String, String> entry : StringByteIterator.getStringMap(values).entrySet()) {
      Tuple v = new Tuple();
      v = v.add(entry.getKey());
      v = v.add(entry.getValue());
      t = t.add(v);
    }
    return t;
  }

  private static Status convTupleToMap(Tuple tuple, Set<String> fields, Map<String, ByteIterator> result) {
    System.err.println("READ SOMETHING!");
    for (int i = 0; i < tuple.size(); i++) {
      Tuple v = tuple.getNestedTuple(i);
      String field = v.getString(0);
      String value = v.getString(1);
      //System.err.println(field + " : " + value);
      result.put(field, new StringByteIterator(value));
    }
    if (fields != null) {
      for (String field : fields) {
        if (result.get(field) == null) {
          System.err.println("field not fount: " + field);
          return Status.NOT_FOUND;
        }
      }
    }
    return Status.OK;
  }

  static <T> CompletableFuture<List<T>> sequence(List<CompletableFuture<T>> com) {
    return CompletableFuture.allOf(com.toArray(new CompletableFuture[com.size()]))
        .thenApply(v -> com.stream()
            .map(CompletableFuture::join)
            .collect(toList())
        );
  }

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one DB instance per client thread.
   */
  @Override
  public void init() throws DBException {
    synchronized (DB.class) {
      if (isFDBInitialized) {
        logger.info("FDB has already been initialized.");
        return;
      }
      initFDB();
    }
  }

  private synchronized void initFDB() throws DBException {
    if(isFDBInitialized){
      logger.info("FDB has already been initialized.");
      return;
    }
    // initialize FoundationDB driver
    final Properties props = getProperties();
    String apiVersion = props.getProperty(API_VERSION, API_VERSION_DEFAULT);
    String clusterFile = props.getProperty(CLUSTER_FILE, CLUSTER_FILE_DEFAULT);
    String dbBatchSize = props.getProperty(DB_BATCH_SIZE, DB_BATCH_SIZE_DEFAULT);

    String datacenterId = props.getProperty(DATACENTER_ID, DATACENTER_ID_DEFAULT);

    String tlsVerifyPeers = props.getProperty(TLS_VERIFY_PEERS, TLS_VERIFY_PEERS_DEFAULT);
    String tlsCertificateFile = props.getProperty(TLS_CERTIFICATE_FILE, TLS_CERTIFICATE_FILE_DEFAULT);
    String tlsKeyFile = props.getProperty(TLS_KEY_FILE, TLS_KEY_FILE_DEFAULT);
    String tlsCaFile = props.getProperty(TLS_CA_FILE, TLS_CA_FILE_DEFAULT);


    logger.info("API Version: {}", apiVersion);
    logger.info("Cluster File: {}", clusterFile);
    logger.info("DB Batch size: {}", dbBatchSize);

    logger.info("tlsVerifyPeers: {}", tlsVerifyPeers);
    logger.info("tlsCertificateFile: {}", tlsCertificateFile);
    logger.info("tlsKeyFile: {}", tlsKeyFile);
    logger.info("tlsCaFile: {}", tlsCaFile);

    try {
      dbName = props.getProperty(DB_NAME, DB_NAME_DEFAULT);
      fdb = FDB.selectAPIVersion(Integer.parseInt(apiVersion.trim()));
      fdb.options().setTLSVerifyPeers(tlsVerifyPeers.getBytes(StandardCharsets.UTF_8));
      fdb.options().setTLSCertPath(tlsCertificateFile);
      fdb.options().setTLSKeyPath(tlsKeyFile);
      fdb.options().setTLSCaPath(tlsCaFile);

      db = fdb.open(clusterFile);
      tenant = db.openTenant(TENANT_NAME);
      batchSize = Integer.parseInt(dbBatchSize);

      if (!Objects.equals(datacenterId, "")) {
        db.options().setDatacenterId(datacenterId);
      }

      isFDBInitialized = true;
    } catch (FDBException e) {
      logger.error(MessageFormatter.format("Error in database operation: {}", "init").getMessage(), e);
      throw new DBException(e);
    } catch (NumberFormatException e) {
      logger.error(MessageFormatter.format("Invalid value for apiversion property: {}", apiVersion).getMessage(), e);
      throw new DBException(e);
    } catch (Exception e) {
      logger.error(MessageFormatter.format("Error in database operation: {}", "init").getMessage(), e);
      throw new DBException(e);
    }
  }
}