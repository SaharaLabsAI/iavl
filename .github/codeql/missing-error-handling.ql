/**
 * @name Missing error handling
 * @description Find function calls that return errors but are not properly handled
 * @kind problem
 * @problem.severity warning
 * @id go/missing-error-handling
 * @tags reliability
 *       maintainability
 */

import go

from CallExpr call, AssignStmt assign
where
  assign.getRhs() = call and
  call.getType().getNumChild() > 1 and
  call.getType().getChild(call.getType().getNumChild() - 1).getName() = "error" and
  assign.getNumLhs() > 1 and
  assign.getLhs(assign.getNumLhs() - 1).(Ident).getName() = "_" and
  // Exclude test files where ignoring errors might be acceptable
  not call.getFile().getBaseName().matches("%_test.go")
select call, "Error return value is ignored - consider proper error handling"