use std::sync::Arc;

use crate::bound::BoundCatalogModifyingStatement;
use crate::error::PlanResult;
use crate::logical_planner::LogicalPlanner;
use crate::plan::PlanNode;
use crate::plan::catalog_modify::{CreateGraph, DropGraph};

impl LogicalPlanner {
    pub fn plan_catalog_modifying_statement(
        &self,
        statement: BoundCatalogModifyingStatement,
    ) -> PlanResult<PlanNode> {
        match statement {
            BoundCatalogModifyingStatement::Call(call) => self.plan_call_procedure_statement(call),
            BoundCatalogModifyingStatement::CreateGraph(create_graph) => {
                self.plan_create_graph_statement(create_graph)
            }
            BoundCatalogModifyingStatement::DropGraph(drop_graph) => {
                self.plan_drop_graph_statement(drop_graph)
            }
            _ => todo!(),
        }
    }

    fn plan_create_graph_statement(
        &self,
        statement: crate::bound::BoundCreateGraphStatement,
    ) -> PlanResult<PlanNode> {
        let plan = CreateGraph::new(statement.name, statement.kind, statement.graph_type);
        Ok(PlanNode::PhysicalCreateGraph(Arc::new(plan)))
    }

    fn plan_drop_graph_statement(
        &self,
        statement: crate::bound::BoundDropGraphStatement,
    ) -> PlanResult<PlanNode> {
        let plan = DropGraph::new(statement.name, statement.if_exists);
        Ok(PlanNode::PhysicalDropGraph(Arc::new(plan)))
    }
}
