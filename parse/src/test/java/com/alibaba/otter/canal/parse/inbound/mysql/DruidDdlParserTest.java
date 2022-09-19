package com.alibaba.otter.canal.parse.inbound.mysql;

import org.junit.Assert;
import org.junit.Test;

import com.alibaba.otter.canal.parse.inbound.mysql.ddl.DdlResult;
import com.alibaba.otter.canal.parse.inbound.mysql.ddl.DruidDdlParser;
import com.alibaba.otter.canal.parse.inbound.mysql.ddl.SimpleDdlParser;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;

public class DruidDdlParserTest {

    @Test
    public void testCreate() {
        String queryString = "CREATE TABLE retl_mark ( `ID` int(11) )";
        DdlResult result = DruidDdlParser.parse(queryString, "retl").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("retl", result.getSchemaName());
        Assert.assertEquals("retl_mark", result.getTableName());

        queryString = "CREATE TABLE IF NOT EXISTS retl.retl_mark ( `ID` int(11) )";
        result = DruidDdlParser.parse(queryString, "retl").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("retl", result.getSchemaName());
        Assert.assertEquals("retl_mark", result.getTableName());

        queryString = "CREATE TABLE IF NOT EXISTS `retl_mark` ( `ID` int(11) )";
        result = DruidDdlParser.parse(queryString, "retl").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("retl", result.getSchemaName());
        Assert.assertEquals("retl_mark", result.getTableName());

        queryString = "CREATE TABLE  `retl`.`retl_mark` (\n  `ID` int(10) unsigned NOT NULL )";
        result = DruidDdlParser.parse(queryString, "retl").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("retl", result.getSchemaName());
        Assert.assertEquals("retl_mark", result.getTableName());

        queryString = "CREATE TABLE  `retl`.`retl_mark`(\n  `ID` int(10) unsigned NOT NULL )";
        result = SimpleDdlParser.parse(queryString, "retl");
        Assert.assertNotNull(result);
        Assert.assertEquals("retl", result.getSchemaName());
        Assert.assertEquals("retl_mark", result.getTableName());

        queryString = "CREATE table `bak591`.`j_order_log_back_201309` like j_order_log";
        result = DruidDdlParser.parse(queryString, "bak").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("bak591", result.getSchemaName());
        Assert.assertEquals("j_order_log_back_201309", result.getTableName());

        queryString = "CREATE DEFINER=sco*erce@% PROCEDURE SC_CPN_CODES_SAVE_ACTION(IN cosmosPassportId CHAR(32), IN orderId CHAR(32), IN codeIds TEXT) BEGIN SET @orderId = orderId; SET @timeNow = NOW(); START TRANSACTION; DELETE FROMsc_ord_couponWHEREORDER_ID= @orderId; SET @i=1; SET @numbers = FN_GET_ELEMENTS_COUNT(codeIds, '|'); WHILE @i <= @numbers DO SET @codeId = FN_FIND_ELEMENT_BYINDEX(codeIds, '|', @i); SET @orderCodeId = UUID32(); INSERT INTOsc_ord_coupon(ID,CREATE_BY,CREATE_TIME,UPDATE_BY,UPDATE_TIME,ORDER_ID,CODE_ID`) VALUES(@orderCodeId, cosmosPassportId, @timeNow, cosmosPassportId, @timeNow, @orderId, @codeId); SET @i = @i + 1; END WHILE; COMMIT; END";
        result = DruidDdlParser.parse(queryString, "bak").get(0);
        Assert.assertEquals(EventType.QUERY, result.getType());

        queryString = "CREATE TABLE performance_schema.cond_instances(`ID` int(10) unsigned NOT NULL ) ";
        result = DruidDdlParser.parse(queryString, "retl").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("performance_schema", result.getSchemaName());
        Assert.assertEquals("cond_instances", result.getTableName());

        queryString = "CREATE TABLE `ob_table` (\n" +
                "  `svr_ip` varchar(46) NOT NULL,\n" +
                "  `svr_port` bigint(20) NOT NULL,\n" +
                "  `tenant_id` bigint(20) NOT NULL,\n" +
                "  `request_id` bigint(20) NOT NULL,\n" +
                "  PRIMARY KEY (`svr_ip`, `svr_port`, `tenant_id`, `request_id`),\n" +
                "  KEY `all_virtual_sql_audit_i1` (`tenant_id`, `request_id`) BLOCK_SIZE 16384 LOCAL\n" +
                ") DEFAULT CHARSET = utf8mb4 ROW_FORMAT = COMPACT COMPRESSION = 'zstd_1.3.8' REPLICA_NUM = 1 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 0\n" +
                " partition by key(svr_ip, svr_port)\n" +
                "(partition p0,\n" +
                "partition p1,\n" +
                "partition p2,\n" +
                "partition p3,\n" +
                "partition p4,\n" +
                "partition p5,\n" +
                "partition p6,\n" +
                "partition p7,\n" +
                "partition p8,\n" +
                "partition p9,\n" +
                "partition p10,\n" +
                "partition p11,\n" +
                "partition p12,\n" +
                "partition p13,\n" +
                "partition p14,\n" +
                "partition p15,\n" +
                "partition p16,\n" +
                "partition p17)";
        result = DruidDdlParser.parse(queryString, "ob").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("ob", result.getSchemaName());
        Assert.assertEquals("ob_table", result.getTableName());
    }

    @Test
    public void testDrop() {
        String queryString = "DROP TABLE retl_mark";
        DdlResult result = DruidDdlParser.parse(queryString, "retl").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("retl", result.getSchemaName());
        Assert.assertEquals("retl_mark", result.getTableName());

        queryString = "DROP TABLE IF EXISTS test.retl_mark;";
        result = DruidDdlParser.parse(queryString, "retl").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("test", result.getSchemaName());
        Assert.assertEquals("retl_mark", result.getTableName());

        queryString = "DROP TABLE IF EXISTS \n \"test\".`retl_mark`;";
        result = DruidDdlParser.parse(queryString, "retl").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("test", result.getSchemaName());
        Assert.assertEquals("retl_mark", result.getTableName());

        queryString = "DROP TABLE IF EXISTS \n retl.retl_mark , retl_test";
        result = DruidDdlParser.parse(queryString, "test").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("retl", result.getSchemaName());
        Assert.assertEquals("retl_mark", result.getTableName());
        result = DruidDdlParser.parse(queryString, "test").get(1);
        Assert.assertNotNull(result);
        Assert.assertEquals("test", result.getSchemaName());
        Assert.assertEquals("retl_test", result.getTableName());

        queryString = "DROP /*!40005 TEMPORARY */ TABLE IF EXISTS `temp_bond_keys`.`temp_bond_key_id`;";
        result = DruidDdlParser.parse(queryString, "retl").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("temp_bond_keys", result.getSchemaName());
        Assert.assertEquals("temp_bond_key_id", result.getTableName());
    }

    @Test
    public void testAlert() {
        String queryString = "alter table retl_mark drop index emp_name";
        DdlResult result = DruidDdlParser.parse(queryString, "retl").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("retl_mark", result.getTableName());

        queryString = "alter table retl.retl_mark drop index emp_name";
        result = DruidDdlParser.parse(queryString, "retl").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("retl_mark", result.getTableName());

        queryString = "alter table \n retl.`retl_mark` drop index emp_name;";
        result = DruidDdlParser.parse(queryString, "retl").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("retl_mark", result.getTableName());

        queryString = "alter table retl.retl_mark drop index emp_name , add index emp_name(id)";
        result = DruidDdlParser.parse(queryString, "retl").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("retl_mark", result.getTableName());
        Assert.assertEquals(EventType.DINDEX, result.getType());

        result = DruidDdlParser.parse(queryString, "retl").get(1);
        Assert.assertNotNull(result);
        Assert.assertEquals("retl_mark", result.getTableName());
        Assert.assertEquals(EventType.CINDEX, result.getType());
    }

    @Test
    public void testTruncate() {
        String queryString = "truncate table retl_mark";
        DdlResult result = DruidDdlParser.parse(queryString, "retl").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("retl_mark", result.getTableName());

        queryString = "truncate table retl.retl_mark";
        result = DruidDdlParser.parse(queryString, "retl").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("retl_mark", result.getTableName());

        queryString = "truncate \n  retl.`retl_mark` ";
        result = DruidDdlParser.parse(queryString, "retl").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("retl_mark", result.getTableName());

        queryString = "truncate \n  retl.retl_mark , retl_test ";
        result = DruidDdlParser.parse(queryString, "retl").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("retl_mark", result.getTableName());
        result = DruidDdlParser.parse(queryString, "retl").get(1);
        Assert.assertNotNull(result);
        Assert.assertEquals("retl_test", result.getTableName());
    }

    @Test
    public void testRename() {
        String queryString = "rename table retl_mark to retl_mark2";
        DdlResult result = DruidDdlParser.parse(queryString, "retl").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("retl", result.getOriSchemaName());
        Assert.assertEquals("retl", result.getSchemaName());
        Assert.assertEquals("retl_mark", result.getOriTableName());
        Assert.assertEquals("retl_mark2", result.getTableName());

        queryString = "rename table retl.retl_mark to retl2.retl_mark2";
        result = DruidDdlParser.parse(queryString, "retl").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("retl", result.getOriSchemaName());
        Assert.assertEquals("retl2", result.getSchemaName());
        Assert.assertEquals("retl_mark", result.getOriTableName());
        Assert.assertEquals("retl_mark2", result.getTableName());

        queryString = "rename \n table \n `retl`.`retl_mark` to `retl2`.`retl_mark2`;";
        result = DruidDdlParser.parse(queryString, "retl").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("retl", result.getOriSchemaName());
        Assert.assertEquals("retl2", result.getSchemaName());
        Assert.assertEquals("retl_mark", result.getOriTableName());
        Assert.assertEquals("retl_mark2", result.getTableName());

        queryString = "rename \n table \n `retl`.`retl_mark` to `retl2`.`retl_mark2` , `retl1`.`retl_mark1` to `retl3`.`retl_mark3`;";
        result = DruidDdlParser.parse(queryString, "retl").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("retl", result.getOriSchemaName());
        Assert.assertEquals("retl2", result.getSchemaName());
        Assert.assertEquals("retl_mark", result.getOriTableName());
        Assert.assertEquals("retl_mark2", result.getTableName());
        result = DruidDdlParser.parse(queryString, "retl").get(1);
        Assert.assertNotNull(result);
        Assert.assertEquals("retl1", result.getOriSchemaName());
        Assert.assertEquals("retl3", result.getSchemaName());
        Assert.assertEquals("retl_mark1", result.getOriTableName());
        Assert.assertEquals("retl_mark3", result.getTableName());

        // 正则匹配test case

        queryString = "rename table totl_mark to totl_mark2";
        result = DruidDdlParser.parse(queryString, "retl").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("retl", result.getOriSchemaName());
        Assert.assertEquals("retl", result.getSchemaName());
        Assert.assertEquals("totl_mark", result.getOriTableName());
        Assert.assertEquals("totl_mark2", result.getTableName());

        queryString = "rename table totl.retl_mark to totl2.retl_mark2";
        result = DruidDdlParser.parse(queryString, "retl").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("totl", result.getOriSchemaName());
        Assert.assertEquals("totl2", result.getSchemaName());
        Assert.assertEquals("retl_mark", result.getOriTableName());
        Assert.assertEquals("retl_mark2", result.getTableName());

        queryString = "rename \n table \n `totl`.`retl_mark` to `totl2`.`retl_mark2`;";
        result = DruidDdlParser.parse(queryString, "retl").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("totl", result.getOriSchemaName());
        Assert.assertEquals("totl2", result.getSchemaName());
        Assert.assertEquals("retl_mark", result.getOriTableName());
        Assert.assertEquals("retl_mark2", result.getTableName());

        queryString = "rename \n table \n `totl`.`retl_mark` to `totl2`.`retl_mark2` , `totl1`.`retl_mark1` to `totl3`.`retl_mark3`;";
        result = DruidDdlParser.parse(queryString, "retl").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("totl", result.getOriSchemaName());
        Assert.assertEquals("totl2", result.getSchemaName());
        Assert.assertEquals("retl_mark", result.getOriTableName());
        Assert.assertEquals("retl_mark2", result.getTableName());
        result = DruidDdlParser.parse(queryString, "retl").get(1);
        Assert.assertNotNull(result);
        Assert.assertEquals("totl1", result.getOriSchemaName());
        Assert.assertEquals("totl3", result.getSchemaName());
        Assert.assertEquals("retl_mark1", result.getOriTableName());
        Assert.assertEquals("retl_mark3", result.getTableName());

    }

    @Test
    public void testIndex() {
        String queryString = "CREATE UNIQUE INDEX index_1 ON retl_mark(id,x)";
        DdlResult result = DruidDdlParser.parse(queryString, "retl").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("retl", result.getSchemaName());
        Assert.assertEquals("retl_mark", result.getTableName());

        queryString = "create index idx_qca_cid_mcid on q_contract_account (contract_id,main_contract_id)";
        result = DruidDdlParser.parse(queryString, "retl").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("retl", result.getSchemaName());
        Assert.assertEquals("q_contract_account", result.getTableName());

        queryString = "DROP INDEX index_str ON retl_mark";
        result = DruidDdlParser.parse(queryString, "retl").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("retl", result.getSchemaName());
        Assert.assertEquals("retl_mark", result.getTableName());
    }

    @Test
    public void testDb() {
        String queryString = "create database db1";
        DdlResult result = DruidDdlParser.parse(queryString, "db0").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("db1", result.getSchemaName());

        queryString = "drop database db1";
        result = DruidDdlParser.parse(queryString, "retl").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("db1", result.getSchemaName());
    }
}
