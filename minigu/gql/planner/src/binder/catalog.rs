use gql_parser::ast::{
    CatalogModifyingStatement, CreateGraphStatement, CreateGraphTypeStatement,
    CreateSchemaStatement, DropGraphStatement, DropGraphTypeStatement, DropSchemaStatement,
};
use minigu_common::error::not_implemented;

use super::Binder;
use super::error::{BindError, BindResult};
use crate::bound::{
    BoundCatalogModifyingStatement, BoundCreateGraphStatement, BoundCreateGraphTypeStatement,
    BoundCreateSchemaStatement, BoundDropGraphStatement, BoundDropGraphTypeStatement,
    BoundDropSchemaStatement,
};

impl Binder<'_> {
    pub fn bind_catalog_modifying_statement(
        &mut self,
        statement: &CatalogModifyingStatement,
    ) -> BindResult<BoundCatalogModifyingStatement> {
        match statement {
            CatalogModifyingStatement::Call(statement) => {
                let statement = self.bind_call_procedure_statement(statement)?;
                if statement.optional {
                    return not_implemented("optional catalog modifying statements", None);
                }
                if statement.schema().is_some() {
                    return Err(BindError::NotCatalogProcedure(statement.name()));
                }
                Ok(BoundCatalogModifyingStatement::Call(statement))
            }
            CatalogModifyingStatement::CreateSchema(statement) => self
                .bind_create_schema_statement(statement)
                .map(BoundCatalogModifyingStatement::CreateSchema),
            CatalogModifyingStatement::DropSchema(statement) => self
                .bind_drop_schema_statement(statement)
                .map(BoundCatalogModifyingStatement::DropSchema),
            CatalogModifyingStatement::CreateGraph(statement) => self
                .bind_create_graph_statement(statement)
                .map(BoundCatalogModifyingStatement::CreateGraph),
            CatalogModifyingStatement::DropGraph(statement) => self
                .bind_drop_graph_statement(statement)
                .map(BoundCatalogModifyingStatement::DropGraph),
            CatalogModifyingStatement::CreateGraphType(statement) => self
                .bind_create_graph_type_statement(statement)
                .map(BoundCatalogModifyingStatement::CreateGraphType),
            CatalogModifyingStatement::DropGraphType(statement) => self
                .bind_drop_graph_type_statement(statement)
                .map(BoundCatalogModifyingStatement::DropGraphType),
        }
    }

    pub fn bind_create_schema_statement(
        &mut self,
        statement: &CreateSchemaStatement,
    ) -> BindResult<BoundCreateSchemaStatement> {
        not_implemented("create schema statement", None)
    }

    pub fn bind_drop_schema_statement(
        &mut self,
        statement: &DropSchemaStatement,
    ) -> BindResult<BoundDropSchemaStatement> {
        not_implemented("drop schema statement", None)
    }

    pub fn bind_create_graph_statement(
        &mut self,
        statement: &CreateGraphStatement,
    ) -> BindResult<BoundCreateGraphStatement> {
        use gql_parser::ast::{CreateGraphOrGraphTypeStatementKind, OfGraphType};

        // Extract graph name
        let name = match statement.path.value().objects.as_slice() {
            [name] => name.value().clone(),
            _ => return Err(BindError::InvalidObjectReference(
                statement.path.value().objects.iter().map(|o| o.value().clone()).collect(),
            )),
        };

        // Bind create kind
        let kind = match statement.kind.value() {
            CreateGraphOrGraphTypeStatementKind::Create => crate::bound::CreateKind::Create,
            CreateGraphOrGraphTypeStatementKind::CreateIfNotExists => {
                crate::bound::CreateKind::CreateIfNotExists
            }
            CreateGraphOrGraphTypeStatementKind::CreateOrReplace => {
                crate::bound::CreateKind::CreateOrReplace
            }
        };

        // Bind graph type
        let graph_type = self.bind_of_graph_type(statement.graph_type.value())?;

        // Bind source (if any)
        let source = if let Some(source_expr) = &statement.source {
            Some(self.bind_graph_expr(source_expr.value())?)
        } else {
            None
        };

        Ok(BoundCreateGraphStatement {
            name,
            kind,
            graph_type,
            source,
        })
    }

    pub fn bind_drop_graph_statement(
        &mut self,
        statement: &DropGraphStatement,
    ) -> BindResult<BoundDropGraphStatement> {
        // Extract graph name
        let name = match statement.path.value().objects.as_slice() {
            [name] => name.value().clone(),
            _ => return Err(BindError::InvalidObjectReference(
                statement.path.value().objects.iter().map(|o| o.value().clone()).collect(),
            )),
        };

        Ok(BoundDropGraphStatement {
            name,
            if_exists: statement.if_exists,
        })
    }

    pub fn bind_create_graph_type_statement(
        &mut self,
        statement: &CreateGraphTypeStatement,
    ) -> BindResult<BoundCreateGraphTypeStatement> {
        not_implemented("create graph type statement", None)
    }

    pub fn bind_drop_graph_type_statement(
        &mut self,
        statement: &DropGraphTypeStatement,
    ) -> BindResult<BoundDropGraphTypeStatement> {
        not_implemented("drop graph type statement", None)
    }

    /// Bind OfGraphType to BoundGraphType
    fn bind_of_graph_type(
        &mut self,
        of_graph_type: &gql_parser::ast::OfGraphType,
    ) -> BindResult<crate::bound::BoundGraphType> {
        use gql_parser::ast::OfGraphType;

        match of_graph_type {
            OfGraphType::Nested(elements) => {
                let bound_elements = elements
                    .iter()
                    .map(|elem| super::type_element::bind_graph_element_type(elem.value()))
                    .collect::<BindResult<Vec<_>>>()?;
                Ok(crate::bound::BoundGraphType::Nested(bound_elements))
            }
            OfGraphType::Ref(graph_type_ref) => {
                // For graph type references, we need to resolve them
                not_implemented("graph type reference", None)
            }
            OfGraphType::Like(_) => {
                not_implemented("LIKE graph type", None)
            }
            OfGraphType::Any => {
                not_implemented("ANY graph type", None)
            }
        }
    }
}
