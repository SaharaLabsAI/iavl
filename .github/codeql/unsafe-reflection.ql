/**
 * @name Unsafe reflection usage
 * @description Find reflection calls that might be unsafe or lead to runtime panics
 * @kind problem
 * @problem.severity warning
 * @id go/unsafe-reflection
 * @tags security
 *       reliability
 */

import go

from CallExpr call
where
  call.getTarget().hasQualifiedName("reflect", ["ValueOf", "TypeOf"]) and
  (
    // Check if the argument might be nil
    call.getArgument(0).(NilLit) or
    // Check if there's no nil check before reflection
    not exists(IfStmt guard |
      guard.getCond().(BinaryExpr).getAnOperand() = call.getArgument(0) and
      guard.getCond().(BinaryExpr).getOperator() = "!="
    )
  )
select call, "Potentially unsafe reflection usage - consider nil checks"