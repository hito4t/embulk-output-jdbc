package org.embulk.output.oracle.oci;

import jnr.ffi.Pointer;
import jnr.ffi.types.u_int16_t;
import jnr.ffi.types.u_int32_t;
import jnr.ffi.types.u_int8_t;

public interface OCI2 {

    short embulk_output_oracle_OCIDirPathColArrayEntriesSet(Pointer dpca,
            Pointer errhp,
            @u_int16_t short columnCount,
            @u_int32_t int rowCount,
            Pointer data,
            Pointer sizes);
}
