/**
 * @name Potential goroutine leak
 * @description Find goroutines that might not be properly cleaned up
 * @kind problem
 * @problem.severity warning
 * @id go/goroutine-leak
 * @tags reliability
 *       concurrency
 */

import go

from GoStmt go_stmt, FuncLit func_lit
where
  go_stmt.getCall().getCallee() = func_lit and
  not exists(SelectStmt select_stmt |
    select_stmt.getEnclosingFunction() = func_lit
  ) and
  not exists(IfStmt if_stmt |
    if_stmt.getEnclosingFunction() = func_lit and
    if_stmt.getCond().(CallExpr).getTarget().hasQualifiedName("context", "Context.Done")
  )
select go_stmt, "This goroutine may not have proper cleanup mechanism"