package org.embulk.output.sqlserver;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.Calendar;

import org.embulk.output.jdbc.BatchInsert;
import org.embulk.output.jdbc.JdbcSchema;
import org.embulk.output.sqlserver.nativeclient.NativeClientWrapper;
import org.embulk.spi.time.Timestamp;

import com.google.common.base.Optional;

public class NativeBatchInsert implements BatchInsert
{
    private NativeClientWrapper client = new NativeClientWrapper();

    private final String server;
    private final int port;
    private final Optional<String> instance;
    private final String database;
    private final Optional<String> user;
    private final Optional<String> password;

    private int columnCount;
    private int lastColumnIndex;

    public NativeBatchInsert(String server, int port, Optional<String> instance,
            String database, Optional<String> user, Optional<String> password)
    {
        this.server = server;
        this.port = port;
        this.instance = instance;
        this.database = database;
        this.user = user;
        this.password = password;

        lastColumnIndex = 0;
    }


    @Override
    public void prepare(String loadTable, JdbcSchema insertSchema) throws SQLException
    {
        columnCount = insertSchema.getCount();
        client.open(server, port, instance, database, user, password, loadTable);
    }

    @Override
    public int getBatchWeight()
    {
        return 0;
    }

    @Override
    public void add() throws IOException, SQLException
    {
        client.sendRow();
    }

    private int nextColumnIndex()
    {
        int nextColumnIndex = lastColumnIndex + 1;
        if (nextColumnIndex == columnCount) {
            lastColumnIndex = 0;
        } else {
            lastColumnIndex++;
        }
        return nextColumnIndex;
    }

    @Override
    public void setNull(int sqlType) throws IOException, SQLException
    {
        client.bindNull(nextColumnIndex());
    }

    @Override
    public void setBoolean(boolean v) throws IOException, SQLException
    {
        System.out.println("#boolean");
    }

    @Override
    public void setByte(byte v) throws IOException, SQLException
    {
        client.bindValue(nextColumnIndex(), v);
    }

    @Override
    public void setShort(short v) throws IOException, SQLException
    {
        client.bindValue(nextColumnIndex(), v);
    }

    @Override
    public void setInt(int v) throws IOException, SQLException
    {
        client.bindValue(nextColumnIndex(), v);
    }

    @Override
    public void setLong(long v) throws IOException, SQLException
    {
        client.bindValue(nextColumnIndex(), v);
    }

    @Override
    public void setFloat(float v) throws IOException, SQLException
    {
        System.out.println("#float");
    }

    @Override
    public void setDouble(double v) throws IOException, SQLException
    {
        System.out.println("#double");
    }

    @Override
    public void setBigDecimal(BigDecimal v) throws IOException, SQLException
    {
        System.out.println("#decimal");
    }

    @Override
    public void setString(String v) throws IOException, SQLException
    {
        client.bindValue(nextColumnIndex(), v);
    }

    @Override
    public void setNString(String v) throws IOException, SQLException
    {
        client.bindValue(nextColumnIndex(), v);
    }

    @Override
    public void setBytes(byte[] v) throws IOException, SQLException
    {
        System.out.println("#bytes");
    }

    @Override
    public void setSqlDate(Timestamp v, Calendar cal) throws IOException, SQLException
    {
        System.out.println("#date");
    }

    @Override
    public void setSqlTime(Timestamp v, Calendar cal) throws IOException, SQLException
    {
        System.out.println("#time");
    }

    @Override
    public void setSqlTimestamp(Timestamp v, Calendar cal) throws IOException, SQLException
    {
        System.out.println("#timestamp");
    }

    @Override
    public void flush() throws IOException, SQLException
    {
        System.out.println("##flush");
    }

    @Override
    public void finish() throws IOException, SQLException
    {
        System.out.println("##finish");
        client.commit(true);
    }

    @Override
    public void close() throws IOException, SQLException
    {
        System.out.println("##close");
        client.close();
    }

}
