#include <string.h>
#include "db2_fdw.h"
#include "DB2ResultColumn.h"

/** global variables  */

/** external variables */
extern char         db2Message[ERRBUFSIZE];/* contains DB2 error messages, set by db2CheckErr()             */
extern int          err_code;              /* error code, set by db2CheckErr()                              */

/** external prototypes */
extern void      db2Error             (db2error sqlstate, const char* message);
extern void      db2Error_d           (db2error sqlstate, const char* message, const char* detail, ...);
extern SQLRETURN db2CheckErr          (SQLRETURN status, SQLHANDLE handle, SQLSMALLINT handleType, int line, char* file);

/** local prototypes */
int db2FetchNext (DB2Session* session, DB2ResultColumn* resultList);

/* db2FetchNext
 * Fetch the next result row, return 1 if there is one, else 0.
 */
int db2FetchNext (DB2Session* session, DB2ResultColumn* resultList) {
  SQLRETURN rc = 0;
  DB2ResultColumn* res = NULL;
  db2Entry1();
  /* make sure there is a statement handle stored in "session" */
  if (session->stmtp == NULL) {
    db2Error (FDW_ERROR, "db2FetchNext internal error: statement handle is NULL");
  }

  /* fetch the next result row */
  rc = SQLFetch(session->stmtp->hsql);
  rc = db2CheckErr(rc, session->stmtp->hsql, session->stmtp->type, __LINE__, __FILE__);
  if (rc != SQL_SUCCESS && rc != SQL_NO_DATA) {
    db2Error_d (err_code == 8177 ? FDW_SERIALIZATION_FAILURE : FDW_UNABLE_TO_CREATE_EXECUTION, "error fetching result: SQLFetch failed to fetch next result row", db2Message);
  }

  if (rc == SQL_SUCCESS) {
    /*
     * Single pass over the bound result columns: copy the driver-owned SQLLEN
     * indicator of the fetched row into the portable per-row state, normalize
     * it and terminate bound text buffers.  (The previous code traversed the
     * result list four times per row.)
     */
    for (res = resultList; res; res = res->next) {
      SQLLEN indicator = 0;

      if (res->unbound)
        continue;
      if (sizeof(indicator) > sizeof(res->val_indicator.bytes))
        db2Error(FDW_ERROR, "db2FetchNext internal error: SQLLEN does not fit into result indicator storage");
      memcpy(&indicator, res->val_indicator.bytes, sizeof(indicator));
      res->val_null = (intptr_t) indicator;

      if (res->val_null == (intptr_t) SQL_NULL_DATA) {
        res->val_len = 0;
      } else if (res->val_null >= 0 && res->val != NULL && res->val_size > 0) {
        /*
         * SQLBindCol reports the payload length through its indicator.  Preserve
         * that bounded length instead of forcing convertTuple() to search for a
         * terminator with strlen().
         */
        res->val_len = (size_t) res->val_null;
        if (res->val_len >= res->val_size)
          res->val_len = res->val_size - 1;
        res->val[res->val_len] = '\0';
      }
    }

    /*
     * Fetch the deliberately unbound columns via SQLGetData.  This is a rare
     * fallback path (see db2PrepareQuery).  Some DB2 CLI/ODBC drivers require
     * SQLGetData calls in strict ascending column order, so db2PrepareQuery
     * stored these columns sorted by resnum in session->getdata_cols; the old
     * code re-scanned the whole result list for every column number on every
     * row, which was O(n^2).
     */
    for (int i = 0; i < session->n_getdata_cols; ++i) {
      SQLLEN ind = 0;
      SQLRETURN get_rc_raw;
      SQLRETURN get_rc;

      res = session->getdata_cols[i];

      if (res->val == NULL || res->val_size == 0) {
        db2Error (FDW_ERROR, "db2FetchNext internal error: result column buffer is NULL");
      }

      /*
       * Some DB2 CLI/ODBC drivers return fixed-width character data without
       * writing a NUL terminator when using SQLGetData(SQL_C_CHAR). Ensure the
       * buffer is pre-zeroed so the string is always terminated even if the
       * driver only overwrites the payload bytes.
       */
      memset(res->val, 0, res->val_size);

      get_rc_raw = SQLGetData(session->stmtp->hsql, (SQLUSMALLINT) res->resnum,
                              SQL_C_CHAR, res->val, (SQLLEN) res->val_size, &ind);
      get_rc = db2CheckErr(get_rc_raw, session->stmtp->hsql, session->stmtp->type, __LINE__, __FILE__);
      if (get_rc != SQL_SUCCESS && get_rc != SQL_NO_DATA) {
        db2Error_d (err_code == 8177 ? FDW_SERIALIZATION_FAILURE : FDW_UNABLE_TO_CREATE_EXECUTION, "error fetching result: SQLGetData failed to fetch column", db2Message);
      }

      if (ind == SQL_NULL_DATA) {
        res->val_null = (intptr_t) SQL_NULL_DATA;
        res->val_len = 0;
        continue;
      }
      if (ind == SQL_NO_TOTAL) {
        /* Best-effort: treat as a C string. */
        res->val[res->val_size - 1] = '\0';
        res->val_len = strlen(res->val);
        res->val_null = (intptr_t) res->val_len;
        continue;
      }

      /* If we got truncation info, grow buffer once and retry. */
      if (get_rc_raw == SQL_SUCCESS_WITH_INFO && ind >= (SQLLEN) res->val_size) {
        size_t needed = (size_t) ind + 1;
        res->val = (char*) db2realloc(needed, res->val, "res->val");
        res->val_size = needed;
        ind = 0;

        memset(res->val, 0, res->val_size);
        get_rc_raw = SQLGetData(session->stmtp->hsql, (SQLUSMALLINT) res->resnum,
                                SQL_C_CHAR, res->val, (SQLLEN) res->val_size, &ind);
        get_rc = db2CheckErr(get_rc_raw, session->stmtp->hsql, session->stmtp->type, __LINE__, __FILE__);
        if (get_rc != SQL_SUCCESS && get_rc != SQL_NO_DATA) {
          db2Error_d (err_code == 8177 ? FDW_SERIALIZATION_FAILURE : FDW_UNABLE_TO_CREATE_EXECUTION, "error fetching result: SQLGetData failed to fetch column", db2Message);
        }
        if (ind == SQL_NULL_DATA) {
          res->val_null = (intptr_t) SQL_NULL_DATA;
          res->val_len = 0;
          continue;
        }
        if (ind == SQL_NO_TOTAL) {
          res->val[res->val_size - 1] = '\0';
          res->val_len = strlen(res->val);
          res->val_null = (intptr_t) res->val_len;
          continue;
        }
      }

      res->val_null = (intptr_t) ind;
      res->val_len = (size_t) ind;
      if (res->val_len >= res->val_size) {
        res->val_len = res->val_size - 1;
      }
      res->val[res->val_len] = '\0';
    }
  }

  db2Exit1(": %d",(rc == SQL_SUCCESS));
  return (rc == SQL_SUCCESS);
}
