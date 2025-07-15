/**
 * @name Potential nil pointer dereference
 * @description Find cases where pointers might be dereferenced without nil checks
 * @kind problem
 * @problem.severity error
 * @id go/nil-dereference
 * @tags reliability
 *       security
 */

import go

from SelectorExpr selector, Variable v
where
  selector.getBase() = v.getARead() and
  v.getType() instanceof PointerType and
  // Check if there's no nil check before dereference
  not exists(IfStmt guard |
    guard.getCond().(BinaryExpr).getAnOperand() = v.getARead() and
    guard.getCond().(BinaryExpr).getOperator() = "!=" and
    guard.getCond().(BinaryExpr).getAnOperand().(NilLit)
  ) and
  // Exclude cases where the variable is known to be assigned
  not exists(AssignStmt assign |
    assign.getLhs() = v.getAReference() and
    assign.getRhs().(CallExpr).getTarget().getName() = "new"
  )
select selector, "Potential nil pointer dereference of variable: " + v.getName()