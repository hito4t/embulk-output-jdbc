package org.embulk.output.sqlserver.nativeclient;

import jnr.ffi.Pointer;

public interface NativeClient
{
    static int SQLCHARACTER = 0x2F;

    static short FAIL = 0;
    static short SUCCEED = 1;
    static int DB_IN = 1;

    short bcp_initW(Pointer hdbc, Pointer szTable, Pointer szDataFile, Pointer szErrorFile, int eDirection);

    short bcp_bind(Pointer hdbc,
            Pointer pData, int cbIndicator, int cbData,
            Pointer pTerm, int cbTerm,
            int eDataType, int idxServerCol);

    short bcp_sendrow(Pointer hdbc);

    int bcp_done(Pointer hdbc);
}
