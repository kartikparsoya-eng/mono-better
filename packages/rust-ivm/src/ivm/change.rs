//! Change types — port of `zql/src/ivm/change.ts`.
//!
//! TS uses tuple types: `Change = [ChangeType, Node, Node | ChildData | null]`.
//! Rust uses an enum — cleaner, type-safe, and the match arms enforce
//! exhaustive handling (like TS's `switch` + `unreachable` default).

use crate::ivm::data::Node;

/// Change type enum — port of TS `ChangeType` (change-type-enum.ts).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ChangeType {
    Add = 0,
    Remove = 1,
    Edit = 2,
    Child = 3,
}

/// Child change data — port of TS `ChildData` (change.ts:12).
#[derive(Clone, Debug)]
pub struct ChildData {
    pub relationship_name: String,
    pub change: Box<Change>,
}

/// A change flowing through the pipeline — port of TS `Change` (change.ts:20).
///
/// TS: `AddChange = [ChangeType.ADD, node: Node, extra: null]`
/// TS: `RemoveChange = [ChangeType.REMOVE, node: Node, extra: null]`
/// TS: `ChildChange = [ChangeType.CHILD, node: Node, child: ChildData]`
/// TS: `EditChange = [ChangeType.EDIT, node: Node, oldNode: Node]`
#[derive(Clone, Debug)]
pub enum Change {
    Add(Node),
    Remove(Node),
    Child {
        node: Node,
        child: ChildData,
    },
    Edit {
        node: Node,
        old_node: Node,
    },
}

impl Change {
    #[inline]
    pub fn change_type(&self) -> ChangeType {
        match self {
            Change::Add(_) => ChangeType::Add,
            Change::Remove(_) => ChangeType::Remove,
            Change::Child { .. } => ChangeType::Child,
            Change::Edit { .. } => ChangeType::Edit,
        }
    }

    #[inline]
    pub fn node(&self) -> &Node {
        match self {
            Change::Add(n) | Change::Remove(n) => n,
            Change::Child { node, .. } => node,
            Change::Edit { node, .. } => node,
        }
    }

    /// Mutable access to the node — used by operators that transform
    /// the node in place (e.g. adding relationships).
    #[inline]
    pub fn node_mut(&mut self) -> &mut Node {
        match self {
            Change::Add(n) | Change::Remove(n) => n,
            Change::Child { node, .. } => node,
            Change::Edit { node, .. } => node,
        }
    }

    #[inline]
    pub fn old_node(&self) -> Option<&Node> {
        match self {
            Change::Edit { old_node, .. } => Some(old_node),
            _ => None,
        }
    }
}

/// Factory functions — port of TS `makeAddChange` etc. (change.ts:48+).
pub fn make_add_change(node: Node) -> Change {
    Change::Add(node)
}

pub fn make_remove_change(node: Node) -> Change {
    Change::Remove(node)
}

pub fn make_child_change(node: Node, child: ChildData) -> Change {
    Change::Child { node, child }
}

pub fn make_edit_change(node: Node, old_node: Node) -> Change {
    Change::Edit { node, old_node }
}

/// Source-level change — port of TS `SourceChange` (source.ts:4).
///
/// TS: `SourceChangeAdd = [ChangeType.ADD, row: Row, extra: null]`
/// TS: `SourceChangeEdit = [ChangeType.EDIT, row: Row, oldRow: Row]`
#[derive(Clone, Debug)]
pub enum SourceChange {
    Add { row: crate::ivm::data::Row },
    Remove { row: crate::ivm::data::Row },
    Edit {
        row: crate::ivm::data::Row,
        old_row: crate::ivm::data::Row,
    },
}

impl SourceChange {
    #[inline]
    pub fn change_type(&self) -> ChangeType {
        match self {
            SourceChange::Add { .. } => ChangeType::Add,
            SourceChange::Remove { .. } => ChangeType::Remove,
            SourceChange::Edit { .. } => ChangeType::Edit,
        }
    }
}

pub fn make_source_change_add(row: crate::ivm::data::Row) -> SourceChange {
    SourceChange::Add { row }
}

pub fn make_source_change_remove(row: crate::ivm::data::Row) -> SourceChange {
    SourceChange::Remove { row }
}

pub fn make_source_change_edit(
    row: crate::ivm::data::Row,
    old_row: crate::ivm::data::Row,
) -> SourceChange {
    SourceChange::Edit { row, old_row }
}
