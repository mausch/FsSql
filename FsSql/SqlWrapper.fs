namespace FsSql

open FsSql

type SqlWrapper(cmgr: Sql.ConnectionManager) =
    member x.ExecReaderF f = Sql.execReaderF cmgr f
    member x.ExecNonQueryF f = Sql.execNonQueryF cmgr f

    member x.ExecReader sql parameters = Sql.execReader cmgr sql parameters
    member x.ExecNonQuery sql parameters = Sql.execNonQuery cmgr sql parameters
    member x.ExecScalar sql parameters = Sql.execScalar cmgr sql parameters

    member x.ExecReaderWith sql parameters mapper = Sql.execReaderWith cmgr sql parameters mapper

    member x.ExecSPReader sql parameters = Sql.execSPReader cmgr sql parameters
    member x.ExecSPNonQuery sql parameters = Sql.execSPNonQuery cmgr sql parameters
    member x.ExecSPScalar sql parameters = Sql.execSPScalar cmgr sql parameters

    member x.AsyncExecReader sql parameters = Sql.asyncExecReader cmgr sql parameters
    member x.AsyncExecNonQuery sql parameters = Sql.asyncExecNonQuery cmgr sql parameters
    member x.AsyncExecScalar sql parameters = Sql.asyncExecScalar cmgr sql parameters

    member x.AsyncExecSPReader sql parameters = Sql.asyncExecSPReader cmgr sql parameters
    member x.AsyncExecSPNonQuery sql parameters = Sql.asyncExecSPNonQuery cmgr sql parameters
    member x.AsyncExecSPScalar sql parameters = Sql.asyncExecSPScalar cmgr sql parameters
