//! Source schema — port of `zql/src/ivm/schema.ts`.

use std::collections::HashMap;

use crate::ivm::data::{Comparator, SortOrder};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum System {
    Permissions,
    Client,
    Test,
}

#[derive(Clone, Debug)]
pub enum ColumnType {
    Boolean { optional: bool },
    Number { optional: bool },
    String { optional: bool },
    Json { optional: bool },
}

#[derive(Clone)]
pub struct SourceSchema {
    pub table_name: String,
    pub columns: HashMap<String, ColumnType>,
    pub primary_key: Vec<String>,
    pub relationships: HashMap<String, SourceSchema>,
    pub relationship_order: Vec<String>,
    pub is_hidden: bool,
    pub system: System,
    pub compare_rows: Comparator,
    pub sort: Option<SortOrder>,
}

impl SourceSchema {
    pub fn with_relationship(
        &self,
        name: &str,
        child_schema: SourceSchema,
        hidden: bool,
        system: System,
    ) -> SourceSchema {
        let mut relationships = self.relationships.clone();
        let mut order = self.relationship_order.clone();

        if !relationships.contains_key(name) {
            order.push(name.to_string());
        }

        let mut child = child_schema;
        child.is_hidden = hidden;
        child.system = system;
        relationships.insert(name.to_string(), child);

        SourceSchema {
            table_name: self.table_name.clone(),
            columns: self.columns.clone(),
            primary_key: self.primary_key.clone(),
            relationships,
            relationship_order: order,
            is_hidden: self.is_hidden,
            system: self.system,
            compare_rows: self.compare_rows.clone(),
            sort: self.sort.clone(),
        }
    }
}
