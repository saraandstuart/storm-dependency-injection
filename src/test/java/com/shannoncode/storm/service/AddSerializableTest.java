package com.shannoncode.storm.service;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.*;

/**
 * @author Stuart Shannon
 */
public class AddSerializableTest
{
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void verifyAddIsNotSerializable() throws Exception
    {
        //given
        thrown.expect(NotSerializableException.class);

        Add original = new Add();
        Add reSerialized = null;

        //when
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(os);
        oos.writeObject(original);
        oos.flush();

        ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(os.toByteArray()));
        reSerialized = (Add) ois.readObject();

        oos.close();
        ois.close();

        //then
        //the expected exception is verified as above

    }
}
