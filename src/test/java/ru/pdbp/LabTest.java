package ru.pdbp;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import java.nio.file.*;
import java.util.*;
import java.io.*;
import org.apache.spark.sql.*;
import static org.junit.jupiter.api.Assertions.*;
class LabTest {
 @TempDir Path dir;
 static SparkSession spark;
 @BeforeAll static void start() { spark = SparkSession.builder().master("local[2]").appName("LabTest")
  .config("spark.driver.host","127.0.0.1").config("spark.driver.bindAddress","127.0.0.1")
  .config("spark.ui.enabled","false").config("spark.sql.shuffle.partitions","2").getOrCreate(); }
 @AfterAll static void stop() { if (spark!=null) spark.stop(); }
 Path file(String body) throws IOException { Path p=dir.resolve(UUID.randomUUID()+".pdbp"); Files.writeString(p, body); return p; }
 Path sample() throws IOException {
  Path p=dir.resolve("sample.pdbp");
  try(var w=new PdbpWriter(p, Codec.DEMO)) {
   w.write(List.of(1L,"Привет|\\\n\r\t🙂", 12.5)); w.write(List.of(2L,"", -2.5));
  } return p;
 }
 @Test void writerHeader() throws Exception { var p=sample(); assertEquals(Codec.DEMO,Codec.schema(p)); assertTrue(Files.readString(p).startsWith("PDBP/1\nid:LONG|name:STRING|amount:DOUBLE\n")); }
 @Test void roundTripAndTypes() throws Exception { try(var r=Codec.open(sample())) { var s=Codec.readHeader(r); var row=Codec.decode(s,r.readLine()); assertArrayEquals(new Object[]{1L,"Привет|\\\n\r\t🙂",12.5},row); assertInstanceOf(Long.class,row[0]); assertInstanceOf(Double.class,row[2]); } }
 @Test void sparkShortNameAndSql() throws Exception {
  var df=spark.read().format("pdbpline").load(sample().toString()); var rows=df.orderBy("id").collectAsList();
  assertEquals(2,rows.size()); assertEquals(1L,rows.get(0).getLong(0)); assertEquals("Привет|\\\n\r\t🙂", rows.get(0).getString(1)); assertEquals(12.5,rows.get(0).getDouble(2));
  assertEquals(1,df.filter("amount > 0").count()); assertEquals(10.0,df.selectExpr("sum(amount)").first().getDouble(0));
  assertTrue(df.queryExecution().executedPlan().toString().contains("BatchScan"));
  assertEquals("long",df.schema().fields()[0].dataType().typeName());
 }
 @Test void emptySpark() throws Exception { var p=dir.resolve("empty.pdbp"); try(var w=new PdbpWriter(p,Codec.DEMO)){} var df=spark.read().format("pdbpline").load(p.toString()); assertEquals(0,df.count()); assertEquals(3,df.schema().size()); }
 @ParameterizedTest @ValueSource(strings={"", "WRONG\nid:LONG\n", "PDBP/2\nid:LONG\n", "PDBP/1\nid:INT\n", "PDBP/1\nid:LONG|id:STRING\n", "PDBP/1\n\n", "PDBP/1\nbad name:LONG\n"})
 void invalidSchema(String body) throws Exception { var p=file(body); assertThrows(IOException.class,()->Codec.schema(p)); assertThrows(Exception.class,()->spark.read().format("pdbpline").load(p.toString())); }
 @ParameterizedTest @ValueSource(strings={"1|x", "1|x|1|extra", "oops|x|1", "9223372036854775808|x|1", "1|x|oops", "1|x|NaN", "1|x|Infinity", "1|x|1e999", "1|bad\\q|1", "1|x|1\\", "1|x|", ""})
 void corruptData(String line) throws Exception { assertThrows(IOException.class,()->Codec.decode(Codec.DEMO,line)); var p=file("PDBP/1\n"+Codec.header(Codec.DEMO)+"\n"+line+"\n"); assertThrows(Exception.class,()->spark.read().format("pdbpline").load(p.toString()).collectAsList()); }
 @Test void writerRejectsTypesAndWidth() throws Exception { try(var w=new PdbpWriter(dir.resolve("bad.pdbp"),Codec.DEMO)) { assertThrows(IllegalArgumentException.class,()->w.write(List.of(1,"x",1.0))); assertThrows(IllegalArgumentException.class,()->w.write(List.of(1L))); assertThrows(IllegalArgumentException.class,()->w.write(List.of(1L,"x",Double.NaN))); } }
 @Test void internalReaderLifecycle() throws Exception { try(var r=new PdbpProvider.PdbpReader(sample(),Codec.DEMO)) { assertThrows(IllegalStateException.class,r::get); assertTrue(r.next()); assertEquals("Привет|\\\n\r\t🙂",r.get().getUTF8String(1).toString()); assertTrue(r.next()); assertFalse(r.next()); assertThrows(IllegalStateException.class,r::get); } }
 @Test void customSchema() throws Exception { var s=new Codec.Schema(List.of(new Codec.Column("title",Codec.Type.STRING),new Codec.Column("key",Codec.Type.LONG))); var p=dir.resolve("custom.pdbp"); try(var w=new PdbpWriter(p,s)){w.write(List.of("имя",Long.MIN_VALUE));} var row=spark.read().format("pdbpline").load(p.toUri().toString()).first(); assertEquals("имя",row.getString(0)); assertEquals(Long.MIN_VALUE,row.getLong(1)); }
 @Test void malformedUtf8() throws Exception { var p=dir.resolve("utf.pdbp"); Files.write(p,new byte[]{(byte)0xc3,0x28}); assertThrows(IOException.class,()->Codec.schema(p)); }
}
