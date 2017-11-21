package com.datatorrent;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Supplier;

import com.datatorrent.cep.schema.Transaction;
import com.datatorrent.unsafe.ResourceHolder;
import com.datatorrent.unsafe.StupidPool;

/**
 * Created by chinmay on 18/11/17.
 */
public class Test
{
  private static Kryo k = new Kryo();
  private static int bufferMaxCapacity = 2000;

  private static StupidPool pool = new StupidPool<>(new Supplier<ByteBuffer>()
  {
    @Override
    public ByteBuffer get()
    {
      return ByteBuffer.allocateDirect(bufferMaxCapacity);
    }
  }, Test.class);

  public static void main(String[] args) throws IOException, NoSuchFieldException
  {
    Transaction t = new Transaction();
    ByteBuffer ser = ser(t);
    System.out.println(ser);

//    TransactionGenerator gen = new TransactionGenerator();
//    gen.setFraudTransactionPercentage(30);
//    gen.setEnrichCustomers(true);
//    gen.setEnrichPaymentCard(true);
//    gen.setEnrichProduct(true);
//    gen.setEnrichStorePOS(true);
//    gen.setCardCount(100);
//
//    gen.generateData();
//
//    for (int i=0; i<1000; i++) {
//      Transaction t1 = gen.generateTransaction(null);
//      ByteBuffer bb = ser(t1);
//      Transaction t2 = (Transaction)de(bb);
//      System.out.println(t1);
//      System.out.println(t2);
//      assert t1.equals(t2);
//    }
  }

  private static Object de(ByteBuffer bb)
  {
    byte[] b = new byte[bufferMaxCapacity];
    bb.rewind();
    bb.get(b, 0, b.length);

    try (Input i = new Input(b)) {
      return k.readObject(i, Transaction.class);
    }
  }

  private static ByteBuffer ser(Object t1) throws IOException
  {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    Output output = new Output(bos);
    k.writeObject(output, t1);
    output.close();

    ResourceHolder take = pool.take();
    ByteBuffer o = (ByteBuffer)take.get();

    o.put(bos.toByteArray());
    return o;
  }
}
