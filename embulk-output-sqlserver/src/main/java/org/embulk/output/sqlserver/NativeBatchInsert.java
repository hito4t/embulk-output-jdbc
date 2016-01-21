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

    public NativeBatchInsert(String server, int port, Optional<String> instance,
            String database, Optional<String> user, Optional<String> password)
    {
        this.server = server;
        this.port = port;
        this.instance = instance;
        this.database = database;
        this.user = user;
        this.password = password;
    }


    @Override
    public void prepare(String loadTable, JdbcSchema insertSchema) throws SQLException
    {
        client.open(server, port, instance, database, user, password);
    }

    @Override
    public int getBatchWeight()
    {
        return 0;
    }

    @Override
    public void add() throws IOException, SQLException {
        // TODO 自動生成されたメソッド・スタブ

    }

    @Override
    public void setNull(int sqlType) throws IOException, SQLException {
        // TODO 自動生成されたメソッド・スタブ

    }

    @Override
    public void setBoolean(boolean v) throws IOException, SQLException {
        // TODO 自動生成されたメソッド・スタブ

    }

    @Override
    public void setByte(byte v) throws IOException, SQLException {
        // TODO 自動生成されたメソッド・スタブ

    }

    @Override
    public void setShort(short v) throws IOException, SQLException {
        // TODO 自動生成されたメソッド・スタブ

    }

    @Override
    public void setInt(int v) throws IOException, SQLException {
        // TODO 自動生成されたメソッド・スタブ

    }

    @Override
    public void setLong(long v) throws IOException, SQLException {
        // TODO 自動生成されたメソッド・スタブ

    }

    @Override
    public void setFloat(float v) throws IOException, SQLException {
        // TODO 自動生成されたメソッド・スタブ

    }

    @Override
    public void setDouble(double v) throws IOException, SQLException {
        // TODO 自動生成されたメソッド・スタブ

    }

    @Override
    public void setBigDecimal(BigDecimal v) throws IOException, SQLException {
        // TODO 自動生成されたメソッド・スタブ

    }

    @Override
    public void setString(String v) throws IOException, SQLException {
        // TODO 自動生成されたメソッド・スタブ

    }

    @Override
    public void setNString(String v) throws IOException, SQLException {
        // TODO 自動生成されたメソッド・スタブ

    }

    @Override
    public void setBytes(byte[] v) throws IOException, SQLException {
        // TODO 自動生成されたメソッド・スタブ

    }

    @Override
    public void setSqlDate(Timestamp v, Calendar cal) throws IOException,
            SQLException {
        // TODO 自動生成されたメソッド・スタブ

    }

    @Override
    public void setSqlTime(Timestamp v, Calendar cal) throws IOException,
            SQLException {
        // TODO 自動生成されたメソッド・スタブ

    }

    @Override
    public void setSqlTimestamp(Timestamp v, Calendar cal) throws IOException,
            SQLException {
        // TODO 自動生成されたメソッド・スタブ

    }

    @Override
    public void flush() throws IOException, SQLException
    {
        // TODO 自動生成されたメソッド・スタブ
    }

    @Override
    public void finish() throws IOException, SQLException
    {
        flush();
    }

    @Override
    public void close() throws IOException, SQLException
    {
        client.close();
    }

}
