package wpmcn;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.Utf8;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * A simple Avro example that serializes and deserializes a pair of strings.
 *
 * @author <a href="bmcneill@intelius.com">W.P. McNeill</a>
 */
public class AvroExample {

   public void serializeGeneric() throws IOException {
      // Create a datum to serialize.
      Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream("/Pair.avsc"));
      GenericRecord datum = new GenericData.Record(schema);
      datum.put("left", new Utf8("dog"));
      datum.put("right", new Utf8("cat"));

      // Serialize it.
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
      Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
      writer.write(datum, encoder);
      encoder.flush();
      out.close();
      System.out.println("Serialization: " + out);

      // Deserialize it.
      DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
      BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
      GenericRecord result = reader.read(null, decoder);
      System.out.printf("Left: %s, Right: %s\n", result.get("left"), result.get("right"));
   }

   public void serializeSpecific() throws IOException {
      // Create a datum to serialize.
      Pair datum = new Pair();
      datum.left = new Utf8("dog");
      datum.right = new Utf8("cat");

      // Serialize it.
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      DatumWriter<Pair> writer = new SpecificDatumWriter<Pair>(Pair.class);
      Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
      writer.write(datum, encoder);
      encoder.flush();
      out.close();
      System.out.println("Serialization: " + out);

      // Deserialize it.
      DatumReader<Pair> reader = new SpecificDatumReader<Pair>(Pair.class);
      BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
      Pair result = reader.read(null, decoder);
      System.out.printf("Left: %s, Right: %s\n", result.left, result.right);
   }

   static public void main(String[] args) throws IOException {
      AvroExample example = new AvroExample();
      System.out.println("Generic");
      example.serializeGeneric();
      System.out.println("Specific");
      example.serializeSpecific();
   }
}
