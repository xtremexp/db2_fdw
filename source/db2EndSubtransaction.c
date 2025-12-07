#include <stdio.h>
#include <sqlcli1.h>
#include <postgres_ext.h>
#include "db2_fdw.h"

/** external variables */
extern char         db2Message[ERRBUFSIZE];/* contains DB2 error messages, set by db2CheckErr()             */
extern DB2EnvEntry* rootenvEntry;          /* Linked list of handles for cached DB2 connections.            */

/** external prototypes */
extern void      db2Debug1            (const char* message, ...);
extern void      db2Debug2            (const char* message, ...);
extern void      db2Error             (db2error sqlstate, const char* message);
extern void      db2Error_d           (db2error sqlstate, const char* message, const char* detail, ...);
extern SQLRETURN db2CheckErr          (SQLRETURN status, SQLHANDLE handle, SQLSMALLINT handleType, int line, char* file);
extern HdlEntry* db2AllocStmtHdl      (SQLSMALLINT type, DB2ConnEntry* connp, db2error error, const char* errmsg);
extern void      db2FreeStmtHdl       (HdlEntry* handlep, DB2ConnEntry* connp);

/** local prototypes */
void             db2EndSubtransaction (void* arg, int nest_level, int is_commit);

/** db2EndSubtransaction
 *   Commit or rollback all subtransaction up to savepoint "nest_nevel".
 *   The first argument must be a connEntry.
 *   If "is_commit" is not true, rollback.
 */
void db2EndSubtransaction (void* arg, int nest_level, int is_commit) {
  SQLCHAR       query[80];
  DB2ConnEntry* con    = (DB2ConnEntry*) arg;
  DB2ConnEntry* connp  = NULL;
  DB2EnvEntry*  envp   = NULL;
  int           found  = 0;
  SQLRETURN     rc     = 0;
  HdlEntry*     hstmtp = NULL;

  db2Debug1("> db2EndSubtransaction");
  /* do nothing if the transaction level is lower than nest_level */
  if (con->xact_level < nest_level)
    return;

  con->xact_level = nest_level - 1;

  /* find the cached handles for the argument */
  for (envp = rootenvEntry; envp != NULL; envp = envp->right) {
    for (connp = envp->connlist; connp != NULL; connp = connp->right) {
      if (connp == con) {
        found = 1;
        break;
      }
    }
  }

  if (!found) {
    /* the code will abend since connp is null*/
    db2Error (FDW_ERROR, "db2EndSubtransaction internal error: handle not found in cache");
  }

  if (is_commit) {
    /*
     * Commit the remote subtransaction by releasing the savepoint.
     * While DB2 automatically releases savepoints on transaction COMMIT,
     * we should explicitly release them when subtransactions complete
     * to free up resources.
     */
    db2Debug2("  release savepoint s%d", nest_level);
    snprintf((char*)query, 79, "RELEASE SAVEPOINT s%d", nest_level);
  } else {
    /*
     * Rollback the remote subtransaction.
     * DB2 requires both ROLLBACK TO and RELEASE SAVEPOINT.
     */
    db2Debug2("  rollback to savepoint s%d and release", nest_level);
    snprintf((char*)query, 79, "ROLLBACK TO SAVEPOINT s%d", nest_level);
  }

  /* create statement handle */
  hstmtp = db2AllocStmtHdl(SQL_HANDLE_STMT, connp, FDW_UNABLE_TO_CREATE_EXECUTION, "error managing savepoint: SQLAllocHandle failed to obtain hstmt");

  /* prepare the query */
  rc = SQLPrepare(hstmtp->hsql, (SQLCHAR*)query, SQL_NTS);
  rc = db2CheckErr(rc,hstmtp->hsql, hstmtp->type, __LINE__, __FILE__);
  if (rc != SQL_SUCCESS) {
    db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error managing savepoint: SQLPrepare failed to prepare savepoint statement", db2Message);
  }

  /* execute savepoint command */
  rc = SQLExecute(hstmtp->hsql);
  rc = db2CheckErr(rc, hstmtp->hsql, hstmtp->type, __LINE__, __FILE__);
  if (rc  != SQL_SUCCESS) {
    db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error managing savepoint: SQLExecute failed to execute savepoint command", db2Message);
  }
  db2FreeStmtHdl(hstmtp, connp);

  /* If we rolled back, also release the savepoint */
  if (!is_commit) {
    db2Debug2("  release savepoint s%d after rollback", nest_level);
    snprintf((char*)query, 79, "RELEASE SAVEPOINT s%d", nest_level);

    /* create statement handle */
    hstmtp = db2AllocStmtHdl(SQL_HANDLE_STMT, connp, FDW_UNABLE_TO_CREATE_EXECUTION, "error releasing savepoint: SQLAllocHandle failed to obtain hstmt");

    /* prepare the query */
    rc = SQLPrepare(hstmtp->hsql, (SQLCHAR*)query, SQL_NTS);
    rc = db2CheckErr(rc,hstmtp->hsql, hstmtp->type, __LINE__, __FILE__);
    if (rc != SQL_SUCCESS) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error releasing savepoint: SQLPrepare failed to prepare release statement", db2Message);
    }

    /* execute release command */
    rc = SQLExecute(hstmtp->hsql);
    rc = db2CheckErr(rc, hstmtp->hsql, hstmtp->type, __LINE__, __FILE__);
    if (rc  != SQL_SUCCESS) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error releasing savepoint: SQLExecute failed to release savepoint", db2Message);
    }
    db2FreeStmtHdl(hstmtp, connp);
  }

  db2Debug1("< db2EndSubtransaction");
}
