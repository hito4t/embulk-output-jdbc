package org.embulk.output.sqlserver.nativeclient;

import java.sql.SQLException;

import jnr.ffi.LibraryLoader;
import jnr.ffi.Pointer;
import jnr.ffi.Runtime;
import jnr.ffi.provider.jffi.ArrayMemoryIO;

import org.embulk.spi.Exec;
import org.slf4j.Logger;

import com.google.common.base.Optional;

public class NativeClientWrapper
{

    private static NativeClient client;

    private final Logger logger = Exec.getLogger(getClass());

    private Pointer envHandle;
    private Pointer odbcHandle;

    private boolean errorOccured;
    private boolean committedOrRollbacked;


    public NativeClientWrapper()
    {
        synchronized (NativeClientWrapper.class) {
            if (client == null) {
                client = loadLibrary();
            }
        }
    }

    private NativeClient loadLibrary()
    {
        logger.info("Loading SQL Server Native Client library.");
        return LibraryLoader.create(NativeClient.class).failImmediately().load("sqlncli11");
    }

    public void open(String server, int port, Optional<String> instance,
            String database, Optional<String> user, Optional<String> password)
                    throws SQLException
    {
        // environment handle
        Pointer envHandlePointer = createPointerPointer();
        check("SQLAllocHandle(SQL_HANDLE_ENV)", client.SQLAllocHandle(
                NativeClient.SQL_HANDLE_ENV,
                null,
                envHandlePointer));
        envHandle = envHandlePointer.getPointer(0);

        // set ODBC version
        check("SQLSetEnvAttr(SQL_ATTR_ODBC_VERSION)", client.SQLSetEnvAttr(
                envHandle,
                NativeClient.SQL_ATTR_ODBC_VERSION,
                Pointer.wrap(Runtime.getSystemRuntime(), NativeClient.SQL_OV_ODBC3),
                NativeClient.SQL_IS_INTEGER));

        // ODBC handle
        Pointer odbcHandlePointer = createPointerPointer();
        check("SQLAllocHandle(SQL_HANDLE_DBC)", client.SQLAllocHandle(
                NativeClient.SQL_HANDLE_DBC,
                envHandle,
                odbcHandlePointer));
        odbcHandle = odbcHandlePointer.getPointer(0);

        // set BULK COPY mode
        check("SQLSetConnectAttr(SQL_COPT_SS_BCP)", client.SQLSetConnectAttrW(
                odbcHandle,
                NativeClient.SQL_COPT_SS_BCP,
                Pointer.wrap(Runtime.getSystemRuntime(), NativeClient.SQL_BCP_ON),
                NativeClient.SQL_IS_INTEGER));

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

        check("SQLDriverConnect", client.SQLDriverConnectW(odbcHandle, null,
                toWideChars(connectionString.toString()), NativeClient.SQL_NTS,
                null, NativeClient.SQL_NTS, null,
                NativeClient.SQL_DRIVER_NOPROMPT));
    }

    public void close()
    {
        if (odbcHandle != null) {
            client.SQLFreeHandle(NativeClient.SQL_HANDLE_DBC, odbcHandle);
            odbcHandle = null;
        }
        if (envHandle != null) {
            client.SQLFreeHandle(NativeClient.SQL_HANDLE_ENV, envHandle);
            envHandle = null;
        }
    }

    private Pointer createPointerPointer()
    {
        return new ArrayMemoryIO(Runtime.getSystemRuntime(), com.kenai.jffi.Type.POINTER.size());
    }

    private void check(String operation, short result) throws SQLException
    {
        switch (result) {
            case NativeClient.SQL_SUCCESS:
            case NativeClient.SQL_SUCCESS_WITH_INFO:
                break;

            default:
                if (odbcHandle != null) {
                    Pointer sqlState = new ArrayMemoryIO(Runtime.getSystemRuntime(), 12);
                    Pointer errorMessage = new ArrayMemoryIO(Runtime.getSystemRuntime(), 512);

                    short getDiagRecResult = client.SQLGetDiagRecW(NativeClient.SQL_HANDLE_DBC, odbcHandle, (short)1,
                            sqlState, null,
                            errorMessage, (short)(errorMessage.size() / 2), null);
                    if (getDiagRecResult == NativeClient.SQL_SUCCESS) {
                        throwException("SQL Server Native Client : %s failed (sql state = %s) : %s", operation, toString(sqlState), toString(errorMessage));
                    }
                }

                throwException("SQL Server Native Client : %s failed : %d.", operation, result);
        }
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


    private void throwException(String format, Object... args) throws SQLException
    {
        errorOccured = true;
        String message = String.format(format, args);
        logger.error(message);
        throw new SQLException(message);
    }
}
