#include <string.h>
#include <stdint.h>
#include "db2_fdw.h"
#include "ParamDesc.h"
#include "DB2ResultColumn.h"

#define SQL_VALUE_PTR_ULEN(v) ((SQLPOINTER)(uintptr_t)(SQLULEN)(v))

/** global variables */

/** external variables */
extern char         db2Message[ERRBUFSIZE];/* contains DB2 error messages, set by db2CheckErr()             */

/** external prototypes */
extern SQLRETURN    db2CheckErr          (SQLRETURN status, SQLHANDLE handle, SQLSMALLINT handleType, int line, char* file);
extern void         db2Error             (db2error sqlstate, const char* message);
extern void         db2Error_d           (db2error sqlstate, const char* message, const char* detail, ...);
extern HdlEntry*    db2AllocStmtHdl      (SQLSMALLINT type, DB2ConnEntry* connp, db2error error, const char* errmsg);
extern SQLSMALLINT  c2param              (SQLSMALLINT fparamType);
extern char*        param2name           (SQLSMALLINT fparamType);

/** internal prototypes */
void                db2PrepareQuery      (DB2Session* session, const char *query, DB2ResultColumn* resultList, unsigned long prefetch, int fetchsize);

/* db2PrepareQuery
 * Prepares an SQL statement for execution.
 * This function should handle everything that has to be done only once even if the statement is executed multiple times, that is:
 * - For SELECT statements, defines the result values to be stored in db2Table.
 * - For DML statements, allocates LOB locators for the RETURNING clause in db2Table.
 * - Set the prefetch options.
 */
void db2PrepareQuery (DB2Session* session, const char *query, DB2ResultColumn* resultList, unsigned long prefetch, int fetchsize) {
  int               col_pos     = 0;
  int               is_select   = 0;
  int               for_update  = 0;
  SQLRETURN         rc          = 0;
  DB2ResultColumn*  res         = NULL;
  int               need_getdata = 0;
  SQLULEN           rowset_alloc = 1;

  /* figure out if the query is a SELECT / SELECT FOR UPDATE */
  is_select  = (strncmp (query, "SELECT", 6) == 0);
  for_update = (strstr (query, "FOR UPDATE") != NULL);

  /*
   * Rowset (block fetch) size used for buffer allocation and binding: the
   * user-configured fetch size, clamped, and reduced to 1 for everything the
   * block-fetch path cannot handle:
   * - DML statements have no result set to fetch rowsets of
   * - SELECT FOR UPDATE uses the dynamic cursor with SQL_ATTR_PREFETCH_NROWS
   * - BLOB/CLOB columns are read with SQLGetData on the current row
   * - statements without result columns use the single-row dummy buffer
   * A bind failure (need_getdata) reduces the final rowset size to 1 after
   * the bind loop below.
   */
  if (fetchsize < 1)
    fetchsize = 1;
  rowset_alloc = (SQLULEN) fetchsize;
  if (rowset_alloc > DB2_MAX_ATTR_ROW_ARRAY_SIZE)
    rowset_alloc = DB2_MAX_ATTR_ROW_ARRAY_SIZE;
  if (!is_select || for_update)
    rowset_alloc = 1;
  if (resultList == NULL)
    rowset_alloc = 1;
  for (res = resultList; res; res = res->next) {
    if (res->colType == SQL_BLOB || res->colType == SQL_CLOB) {
      rowset_alloc = 1;
      break;
    }
  }

  db2Entry1();
  db2Debug2("query    : '%s'",query);
  db2Debug2("prefetch : %d",prefetch);
  db2Debug2("fetchsize: %d",fetchsize);
  /* make sure there is no statement handle stored in "session" */
  if (session->stmtp != NULL) {
    db2Error(FDW_ERROR, "db2PrepareQuery internal error: statement handle is not NULL");
  }

  /* create statement handle */
  session->stmtp = db2AllocStmtHdl(SQL_HANDLE_STMT, session->connp, FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: failed to allocate statement handle");
  db2Debug2("session->stmtp->hsql: %d",session->stmtp->hsql);
  /* set cursor type options */
  if (is_select) {
    db2Debug3("IS_SELECT");
    if (for_update) {
      db2Debug3("FOR UPDATE");
      // Make the cursor sensitive scrollable (e.g., static) so PREFETCH_NROWS applies
      rc = SQLSetStmtAttr(session->stmtp->hsql, SQL_ATTR_CURSOR_TYPE, (SQLPOINTER)SQL_CURSOR_DYNAMIC, 0);
      rc = db2CheckErr(rc, session->stmtp->hsql, session->stmtp->type, __LINE__, __FILE__);
      if (rc != SQL_SUCCESS) {
        db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: SQLSetStmtAttr failed to make cursor dynamic", db2Message);
      }
      db2Debug3("set cursor dynamic");
      rc = SQLSetStmtAttr(session->stmtp->hsql, SQL_ATTR_CONCURRENCY, (SQLPOINTER)SQL_CONCUR_LOCK, 0);
      rc = db2CheckErr(rc, session->stmtp->hsql, session->stmtp->type, __LINE__, __FILE__);
      if (rc != SQL_SUCCESS) {
        db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: SQLSetStmtAttr failed to make cursor pessemistic", db2Message);
      }
      db2Debug3("set cursor pessemistic");
    } else {
      /*
       * Keep the forward-only cursor introduced in 18.2.  Several DB2 CLI
       * setups reject the former static cursor/prefetch combination with
       * CLI0111E / SQLSTATE 22003.
       */
      rc = SQLSetStmtAttr(session->stmtp->hsql, SQL_ATTR_CURSOR_TYPE, (SQLPOINTER)SQL_CURSOR_FORWARD_ONLY, 0);
      rc = db2CheckErr(rc, session->stmtp->hsql, session->stmtp->type, __LINE__, __FILE__);
      if (rc != SQL_SUCCESS) {
        db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: SQLSetStmtAttr failed to make cursor forward-only", db2Message);
      }
      db2Debug3("set cursor forward-only");
    }
  }

  /* prepare the statement */
  db2Debug2("query to prepare: '%s'",query);
  rc = SQLPrepare(session->stmtp->hsql, (SQLCHAR*)query, SQL_NTS);
  rc = db2CheckErr(rc, session->stmtp->hsql, session->stmtp->type, __LINE__, __FILE__);
  if (rc != SQL_SUCCESS) {
    db2Error_d(FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: SQLPrepare failed to prepare remote query", db2Message);
  }

  /* loop through expected result columns */
  for (res = resultList; res; res = res->next){
    SQLSMALLINT fparamType = c2param((SQLSMALLINT)res->colType);
    size_t needed = 0;
    /* Unfortunately DB2 handles DML statements with a RETURNING clause quite different from SELECT statements.
     * In the latter, the result columns are "defined", i.e. bound to some storage space.
     * This definition is only necessary once, even if the query is executed multiple times, so we do this here.
     * RETURNING clause are handled in db2ExecuteQuery, here we only allocate locators for LOB columns in RETURNING clauses.
     */
    /* figure out in which format we want the results */
    if (res->pgtype == UUIDOID) {
      fparamType = SQL_C_CHAR;
    }

    /*
     * Numeric result columns (DECIMAL/NUMERIC/DECFLOAT) are bound like every
     * other column, as SQL_C_CHAR.  They used to be left unbound and fetched
     * with SQLGetData per row, which is by far the slowest retrieval method
     * and also forced single-row fetching.  The CLI0111E / SQLSTATE 22003
     * errors that motivated this were caused by conversion into too-small
     * output buffers, so size the buffer for the maximal textual
     * representation instead:
     *  - precision digits
     *  - optional sign
     *  - optional decimal point
     *  - NUL terminator
     *  - headroom for driver-specific formatting differences
     * For DECFLOAT, use a conservative minimum to accommodate exponent forms.
     */
    if (res->colType == SQL_DECIMAL || res->colType == SQL_NUMERIC || res->colType == SQL_DECFLOAT) {
      size_t prec = (res->colSize > 0 ? res->colSize : 32);
      size_t scale = (res->colScale > 0 ? res->colScale : 0);
      needed = prec + 2 /* sign + NUL */ + (scale > 0 ? 1 /* '.' */ : 0) + 8 /* headroom */;
      if (res->colType == SQL_DECFLOAT && needed < 72)
        needed = 72;

      if (res->val_size < needed) {
        res->val = (char*) db2realloc(needed, res->val, "res->val");
        res->val_size = needed;
        res->val_alloc_bytes = needed;
      }
    }

    /*
     * Allocate (or reuse across re-prepares) the rowset buffers of this
     * column.  With rowset fetching, "val" holds rowset_alloc rows of
     * val_size+1 bytes each (the spare byte per row keeps the terminating 0
     * written by convertTuple within the row's own stride), and "val_ind"
     * holds one SQLLEN indicator per row for column-wise binding.
     */
    if (rowset_alloc > 1) {
      size_t needed_bytes = (res->val_size + 1) * rowset_alloc;
      if (res->val_alloc_bytes < needed_bytes) {
        res->val = (char*) db2realloc(needed_bytes, res->val, "res->val rowset");
        res->val_alloc_bytes = needed_bytes;
      }
    }
    if (res->val_ind_rows < (int) rowset_alloc) {
      if (res->val_ind == NULL)
        res->val_ind = db2alloc(sizeof(SQLLEN) * rowset_alloc, "res->val_ind");
      else
        res->val_ind = db2realloc(sizeof(SQLLEN) * rowset_alloc, res->val_ind, "res->val_ind");
      res->val_ind_rows = (int) rowset_alloc;
    }

    db2Debug2("res->colName       : %s" ,res->colName);
    db2Debug2("res->colSize       : %ld",res->colSize);
    db2Debug2("res->colType       : %d" ,res->colType);
    db2Debug2("res->colScale      : %d" ,res->colScale);
    db2Debug2("res->colNulls      : %d" ,res->colNulls);
    db2Debug2("res->colChars      : %ld",res->colChars);
    db2Debug2("res->colBytes      : %ld",res->colBytes);
    db2Debug2("res->colPrimKeyPart: %d" ,res->colPrimKeyPart);
    db2Debug2("res->colCodepage   : %d" ,res->colCodepage);
    db2Debug2("res->val           : %x" ,res->val);
    db2Debug2("res->val_size      : %ld",res->val_size);
    db2Debug2("res->val_len       : %ld" ,(long) res->val_len);
    db2Debug2("res->val_null      : %ld" ,(long) res->val_null);
    db2Debug2("res->resnum        : %d" ,res->resnum);
    db2Debug2("fparamType: %d (%s)",fparamType,param2name(fparamType));

    if (res->unbound) {
      /* the column is deliberately unbound and will be fetched via SQLGetData
       * in db2FetchNext (set by a previous prepare or by a failed bind below) */
      res->val_null = (intptr_t) SQL_NULL_DATA;
      res->val_len = 0;
    } else {
      db2Debug2("SQLBindCol(%d,%d,%d(%s),%x,%ld,%x)",session->stmtp->hsql,res->resnum, fparamType, param2name(fparamType), res->val, res->val_size, res->val_ind);
      if (rowset_alloc > 1) {
        /* column-wise rowset binding: one buffer of val_size+1 bytes and one
         * SQLLEN indicator per row of the rowset */
        rc = SQLBindCol (session->stmtp->hsql,res->resnum, fparamType, res->val, (SQLLEN)(res->val_size + 1), (SQLLEN*) res->val_ind);
      } else {
        rc = SQLBindCol (session->stmtp->hsql,res->resnum, fparamType, res->val, res->val_size, (SQLLEN*) res->val_ind);
      }
      rc = db2CheckErr(rc, session->stmtp->hsql, session->stmtp->type, __LINE__, __FILE__);
      if (rc == SQL_ERROR) {
        /*
         * Some DB2 CLI setups refuse to bind certain column types.  Leave the
         * column unbound and fetch it via SQLGetData in db2FetchNext instead
         * of failing the whole query.
         */
        db2Debug2("SQLBindCol failed for result column %d (%s), fetching it via SQLGetData instead", res->resnum, res->colName ? res->colName : "?");
        res->unbound = 1;
        need_getdata = 1;
        res->val_null = (intptr_t) SQL_NULL_DATA;
        res->val_len = 0;
      }
    }
    col_pos++;
  }

  /*
   * Unbound columns are read with SQLGetData, which works on the single row
   * the cursor is currently positioned on, so they force single-row fetching.
   * Index them sorted by resnum once per prepare: some DB2 CLI/ODBC drivers
   * require SQLGetData calls in strict ascending column order, and this
   * spares db2FetchNext from re-scanning the result list for every column
   * number on every row (which was O(n^2)).
   */
  if (need_getdata) {
    DB2ResultColumn** cols = NULL;
    int               n    = 0;
    int               i    = 0;
    int               j    = 0;

    for (res = resultList; res; res = res->next) {
      if (res->unbound)
        ++n;
    }
    cols = (DB2ResultColumn**) db2alloc(sizeof(DB2ResultColumn*) * n, "session->getdata_cols");
    i = 0;
    for (res = resultList; res; res = res->next) {
      if (res->unbound)
        cols[i++] = res;
    }
    /* insertion sort by resnum (ascending) */
    for (i = 1; i < n; ++i) {
      DB2ResultColumn* tmp = cols[i];
      for (j = i - 1; j >= 0 && cols[j]->resnum > tmp->resnum; --j)
        cols[j + 1] = cols[j];
      cols[j + 1] = tmp;
    }
    session->getdata_cols   = cols;
    session->n_getdata_cols = n;
  } else {
    session->getdata_cols   = NULL;
    session->n_getdata_cols = 0;
  }

  db2Debug2("is_select: %s",is_select ? "true" : "false");
  db2Debug2("col_pos: %d",col_pos);
  if (is_select && col_pos == 0) {
    /* No columns selected (i.e., SELECT '1' FROM or COUNT(*)).
     * Use persistent buffers from statement handle to avoid stack deallocation issues.
     * This fixes the segfault when using aggregate functions without WHERE clause.
     */
    rc = SQLBindCol(session->stmtp->hsql, 1, SQL_C_CHAR, session->stmtp->dummy_buffer, sizeof(session->stmtp->dummy_buffer), &session->stmtp->dummy_null);
    rc = db2CheckErr(rc, session->stmtp->hsql, session->stmtp->type, __LINE__, __FILE__);
    if (rc != SQL_SUCCESS) {
      db2Error_d ( FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: SQLBindCol failed to define result value", db2Message);
    }
  }

  /*
   * Final rowset size: the allocation size, reduced to 1 when any column had
   * to be left unbound.  Set the rowset statement attributes and reset the
   * rowset state of the session for the new statement.
   */
  {
    SQLULEN rowset_size = rowset_alloc;

    if (need_getdata)
      rowset_size = 1;

    if (is_select) {
      SQLULEN prefetch_rows = prefetch;

      rc = SQLSetStmtAttr(session->stmtp->hsql, SQL_ATTR_ROW_ARRAY_SIZE, SQL_VALUE_PTR_ULEN(rowset_size), 0);
      rc = db2CheckErr(rc, session->stmtp->hsql, session->stmtp->type, __LINE__, __FILE__);
      if (rc != SQL_SUCCESS) {
        db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: SQLSetStmtAttr failed to set fetchsize in statement handle", db2Message);
      }
      db2Debug2("set cursor fetchsize: %d",rowset_size);

      /* the driver reports the number of rows actually fetched per block and
       * their status, so partial rowsets at the end of the result set are
       * handled correctly */
      rc = SQLSetStmtAttr(session->stmtp->hsql, SQL_ATTR_ROWS_FETCHED_PTR, (SQLPOINTER) &session->rows_fetched, 0);
      rc = db2CheckErr(rc, session->stmtp->hsql, session->stmtp->type, __LINE__, __FILE__);
      if (rc != SQL_SUCCESS) {
        db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: SQLSetStmtAttr failed to set rows-fetched pointer in statement handle", db2Message);
      }
      if (session->row_status_rows < rowset_size) {
        if (session->row_status == NULL)
          session->row_status = (SQLUSMALLINT*) db2alloc(sizeof(SQLUSMALLINT) * rowset_size, "session->row_status");
        else
          session->row_status = (SQLUSMALLINT*) db2realloc(sizeof(SQLUSMALLINT) * rowset_size, session->row_status, "session->row_status");
        session->row_status_rows = rowset_size;
      }
      rc = SQLSetStmtAttr(session->stmtp->hsql, SQL_ATTR_ROW_STATUS_PTR, (SQLPOINTER) session->row_status, 0);
      rc = db2CheckErr(rc, session->stmtp->hsql, session->stmtp->type, __LINE__, __FILE__);
      if (rc != SQL_SUCCESS) {
        db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: SQLSetStmtAttr failed to set row status pointer in statement handle", db2Message);
      }

      /* PREFETCH_NROWS only applies to the scrollable FOR UPDATE cursor. */
      if (for_update) {
        rc = SQLSetStmtAttr(session->stmtp->hsql, SQL_ATTR_PREFETCH_NROWS, SQL_VALUE_PTR_ULEN(prefetch_rows), 0);
        rc = db2CheckErr(rc, session->stmtp->hsql, session->stmtp->type, __LINE__, __FILE__);
        if (rc != SQL_SUCCESS) {
          db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: SQLSetStmtAttr failed to set number of prefetched rows in statement handle", db2Message);
        }
        db2Debug2("set cursor prefetch: %d",prefetch_rows);
      }
    }

    session->rowset_size = rowset_size;
    session->rowset_rows = 0;
    session->cur_row     = 0;
  }
  db2Exit1();
}
