package org.embulk.output.sqlserver.nativeclient;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import jnr.ffi.LibraryLoader;
import jnr.ffi.Pointer;
import jnr.ffi.Runtime;
import jnr.ffi.provider.jffi.ArrayMemoryIO;

import org.embulk.spi.Exec;
import org.slf4j.Logger;

import com.google.common.base.Optional;

public class NativeClientWrapper
{
    private static ODBC odbc;
    private static NativeClient client;

    private final Logger logger = Exec.getLogger(getClass());

    private Charset charset;
    private Pointer envHandle;
    private Pointer odbcHandle;

    private Map<Integer, Pointer> boundPointers = new HashMap<Integer, Pointer>();

    public NativeClientWrapper()
    {
        synchronized (NativeClientWrapper.class) {
            if (odbc == null) {
                logger.info("Loading SQL Server Native Client library (odbc32).");
                odbc = LibraryLoader.create(ODBC.class).failImmediately().load("odbc32");
            }
            if (client == null) {
                logger.info("Loading SQL Server Native Client library (sqlncli11).");
                client = LibraryLoader.create(NativeClient.class).failImmediately().load("sqlncli11");
            }
        }

        charset = Charset.forName("MS932");
    }

    public void open(String server, int port, Optional<String> instance,
            String database, Optional<String> user, Optional<String> password,
            String table)
                    throws SQLException
    {
        // environment handle
        Pointer envHandlePointer = createPointerPointer();
        checkSQLResult("SQLAllocHandle(SQL_HANDLE_ENV)", odbc.SQLAllocHandle(
                ODBC.SQL_HANDLE_ENV,
                null,
                envHandlePointer));
        envHandle = envHandlePointer.getPointer(0);

        // set ODBC version
        checkSQLResult("SQLSetEnvAttr(SQL_ATTR_ODBC_VERSION)", odbc.SQLSetEnvAttr(
                envHandle,
                ODBC.SQL_ATTR_ODBC_VERSION,
                Pointer.wrap(Runtime.getSystemRuntime(), ODBC.SQL_OV_ODBC3),
                ODBC.SQL_IS_INTEGER));

        // ODBC handle
        Pointer odbcHandlePointer = createPointerPointer();
        checkSQLResult("SQLAllocHandle(SQL_HANDLE_DBC)", odbc.SQLAllocHandle(
                ODBC.SQL_HANDLE_DBC,
                envHandle,
                odbcHandlePointer));
        odbcHandle = odbcHandlePointer.getPointer(0);

        // set BULK COPY mode
        checkSQLResult("SQLSetConnectAttr(SQL_COPT_SS_BCP)", odbc.SQLSetConnectAttrW(
                odbcHandle,
                ODBC.SQL_COPT_SS_BCP,
                Pointer.wrap(Runtime.getSystemRuntime(), ODBC.SQL_BCP_ON),
                ODBC.SQL_IS_INTEGER));

        StringBuilder connectionString = new StringBuilder();
        connectionString.append("Driver={SQL Server Native Client 11.0};");
        if (instance.isPresent()) {
            connectionString.append(String.format("Server=%s,%d\\%s;", server, port, instance.get()));
        } else {
            connectionString.append(String.format("Server=%s,%d;", server, port));
        }
        connectionString.append(String.format("Database=%s;", database));
        if (user.isPresent()) {
            connectionString.append(String.format("UID=%s;", user.get()));
        }
        if (password.isPresent()) {
            logger.info("connection string = " + connectionString + "PWD=********;");
            connectionString.append(String.format("PWD=%s;", password.get()));
        } else {
            logger.info("connection string = " + connectionString);
        }

        checkSQLResult("SQLDriverConnect", odbc.SQLDriverConnectW(
                odbcHandle,
                null,
                toWideChars(connectionString.toString()),
                ODBC.SQL_NTS,
                null,
                ODBC.SQL_NTS,
                null,
                ODBC.SQL_DRIVER_NOPROMPT));

        StringBuilder fullTableName = new StringBuilder();
        fullTableName.append("[");
        fullTableName.append(database);
        fullTableName.append("].");
        fullTableName.append(".[");
        fullTableName.append(table);
        fullTableName.append("]");
        checkBCPResult("bcp_init", client.bcp_initW(
                odbcHandle,
                toWideChars(fullTableName.toString()),
                null,
                null,
                NativeClient.DB_IN));
    }

    public void bindNull(int columnIndex) throws SQLException
    {
        Pointer pointer = prepareBuffer(columnIndex, 0);
        checkBCPResult("bcp_bind", client.bcp_bind(
                odbcHandle,
                pointer,
                0,
                NativeClient.SQL_NULL_DATA,
                null,
                0,
                NativeClient.SQLCHARACTER,
                columnIndex));

    }

    public void bindValue(int columnIndex, String value) throws SQLException
    {
        ByteBuffer bytes = charset.encode(value);
        Pointer pointer = prepareBuffer(columnIndex, bytes.remaining());
        pointer.put(0, bytes.array(), 0, bytes.remaining());

        checkBCPResult("bcp_bind", client.bcp_bind(
                odbcHandle,
                pointer,
                0,
                (int)pointer.size(),
                null,
                0,
                NativeClient.SQLCHARACTER,
                columnIndex));

    }

    public void bindValue(int columnIndex, boolean value) throws SQLException
    {
        Pointer pointer = prepareBuffer(columnIndex, 1);
        pointer.putByte(0, value ? (byte)1 : (byte)0);

        checkBCPResult("bcp_bind", client.bcp_bind(
                odbcHandle,
                pointer,
                0,
                (int)pointer.size(),
                null,
                0,
                NativeClient.SQLBIT,
                columnIndex));

    }

    public void bindValue(int columnIndex, byte value) throws SQLException
    {
        Pointer pointer = prepareBuffer(columnIndex, 1);
        pointer.putByte(0, value);

        checkBCPResult("bcp_bind", client.bcp_bind(
                odbcHandle,
                pointer,
                0,
                (int)pointer.size(),
                null,
                0,
                NativeClient.SQLINT1,
                columnIndex));

    }

    public void bindValue(int columnIndex, short value) throws SQLException
    {
        Pointer pointer = prepareBuffer(columnIndex, 2);
        pointer.putShort(0, value);

        checkBCPResult("bcp_bind", client.bcp_bind(
                odbcHandle,
                pointer,
                0,
                (int)pointer.size(),
                null,
                0,
                NativeClient.SQLINT2,
                columnIndex));

    }

    public void bindValue(int columnIndex, int value) throws SQLException
    {
        Pointer pointer = prepareBuffer(columnIndex, 4);
        pointer.putInt(0, value);

        checkBCPResult("bcp_bind", client.bcp_bind(
                odbcHandle,
                pointer,
                0,
                (int)pointer.size(),
                null,
                0,
                NativeClient.SQLINT4,
                columnIndex));

    }

    public void bindValue(int columnIndex, long value) throws SQLException
    {
        Pointer pointer = prepareBuffer(columnIndex, 8);
        pointer.putLongLong(0, value);

        checkBCPResult("bcp_bind", client.bcp_bind(
                odbcHandle,
                pointer,
                0,
                (int)pointer.size(),
                null,
                0,
                NativeClient.SQLINT8,
                columnIndex));

    }

    public void bindValue(int columnIndex, float value) throws SQLException
    {
        Pointer pointer = prepareBuffer(columnIndex, 4);
        pointer.putFloat(0, value);

        checkBCPResult("bcp_bind", client.bcp_bind(
                odbcHandle,
                pointer,
                0,
                (int)pointer.size(),
                null,
                0,
                NativeClient.SQLFLT4,
                columnIndex));

    }

    public void bindValue(int columnIndex, double value) throws SQLException
    {
        Pointer pointer = prepareBuffer(columnIndex, 8);
        pointer.putDouble(0, value);

        checkBCPResult("bcp_bind", client.bcp_bind(
                odbcHandle,
                pointer,
                0,
                (int)pointer.size(),
                null,
                0,
                NativeClient.SQLFLT8,
                columnIndex));

    }

    private Pointer prepareBuffer(int columnIndex, int size)
    {
        Pointer pointer = boundPointers.get(columnIndex);
        if (pointer == null || pointer.size() < size) {
            Runtime runtime = Runtime.getSystemRuntime();
            pointer = Pointer.wrap(runtime, ByteBuffer.allocateDirect(size).order(runtime.byteOrder()));
            boundPointers.put(columnIndex, pointer);
        }
        return pointer;
    }

    public void sendRow() throws SQLException
    {
        checkBCPResult("bcp_sendrow", client.bcp_sendrow(odbcHandle));
    }

    public void commit(boolean done) throws SQLException
    {
        int result = client.bcp_done(odbcHandle);
        if (result < 0) {
            throwException("bcp_done", NativeClient.FAIL);
        } else {
            logger.info(String.format("SQL Server Native Client : %,d rows have bean loaded.", result));
        }

    }

    public void close()
    {
        if (odbcHandle != null) {
            odbc.SQLFreeHandle(ODBC.SQL_HANDLE_DBC, odbcHandle);
            odbcHandle = null;
        }
        if (envHandle != null) {
            odbc.SQLFreeHandle(ODBC.SQL_HANDLE_ENV, envHandle);
            envHandle = null;
        }
    }

    private Pointer createPointerPointer()
    {
        return new ArrayMemoryIO(Runtime.getSystemRuntime(), com.kenai.jffi.Type.POINTER.size());
    }

    private String toString(Pointer wcharPointer)
    {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < wcharPointer.size(); i += 2) {
            short c = wcharPointer.getShort(i);
            if (c == 0) {
                break;
            }
            builder.append((char)c);
        }
        return builder.toString();
    }

    private Pointer toWideChars(String s)
    {
        Pointer pointer = new ArrayMemoryIO(Runtime.getSystemRuntime(), (s.length() + 1) * 2);
        for (int i = 0; i < s.length(); i++) {
            pointer.putShort(i * 2, (short)s.charAt(i));
        }
        pointer.putShort(s.length() * 2, (short)0);
        return pointer;
    }

    private Pointer toChars(String s)
    {
        return Pointer.wrap(Runtime.getSystemRuntime(), ByteBuffer.wrap(s.getBytes()));
    }

    private void checkSQLResult(String operation, short result) throws SQLException
    {
        switch (result) {
            case ODBC.SQL_SUCCESS:
                break;

            case ODBC.SQL_SUCCESS_WITH_INFO:
                StringBuilder sqlState = new StringBuilder();
                StringBuilder sqlMessage = new StringBuilder();
                if (getErrorMessage(sqlState, sqlMessage)) {
                    logger.info(String.format("SQL Server Native Client : %s : %s", operation, sqlMessage));
                }
                break;

            default:
                throwException(operation, result);
        }
    }

    private void checkBCPResult(String operation, short result) throws SQLException
    {
        switch (result) {
            case NativeClient.SUCCEED:
                break;

            default:
                throwException(operation, result);
        }
    }

    private void throwException(String operation, short result) throws SQLException
    {
        String message = String.format("SQL Server Native Client : %s failed : %d.", operation, result);

        if (odbcHandle != null) {
            StringBuilder sqlState = new StringBuilder();
            StringBuilder sqlMessage = new StringBuilder();
            if (getErrorMessage(sqlState, sqlMessage)) {
                message = String.format("SQL Server Native Client : %s failed (sql state = %s) : %s", operation, sqlState, sqlMessage);
            }
        }

        logger.error(message);
        throw new SQLException(message);
    }

    private boolean getErrorMessage(StringBuilder sqlState, StringBuilder sqlMessage)
    {
        // (5 (SQL state length) + 1 (terminator length)) * 2 (wchar size)
        Pointer sqlStatePointer = new ArrayMemoryIO(Runtime.getSystemRuntime(), 12);
        Pointer sqlMessagePointer = new ArrayMemoryIO(Runtime.getSystemRuntime(), 512);

        for (short record = 1;; record++) {
            short result = odbc.SQLGetDiagRecW(
                    ODBC.SQL_HANDLE_DBC,
                    odbcHandle,
                    record,
                    sqlStatePointer,
                    null,
                    sqlMessagePointer,
                    (short)(sqlMessagePointer.size() / 2),
                    null);

            if (result == ODBC.SQL_SUCCESS) {
                if (record > 1) {
                    sqlState.append(",");
                }
                sqlState.append(toString(sqlStatePointer));
                sqlMessage.append(toString(sqlMessagePointer));
            } else {
                if (record == 1) {
                    return false;
                }
                break;
            }
        }

        return true;
    }

}
