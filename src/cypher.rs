#[allow(clippy::wildcard_imports)]
use libcypher_parser_sys::*;
use std::ffi::CStr;
use std::os::raw::c_char;

// Re-export pattern graph types from the pattern_graph module
#[allow(unused_imports)]
pub use crate::pattern_graph::{
    EdgeIndex, GraphError, MatchGraphResult, PatternEdge, PatternGraph, PatternPath, PatternPaths,
    PatternVertex, RelationshipDirection, SpanningTreeEdge, TraversalDirection, extract_identifier,
    make_match_graph, print_pattern_graph,
};

/// Prints the AST structure recursively with indentation
#[allow(dead_code)]
pub fn print_ast(node: *const cypher_astnode_t, indent: usize) {
    if node.is_null() {
        println!("{}<null>", "  ".repeat(indent));
        return;
    }

    let node_type = unsafe { cypher_astnode_type(node) };
    let type_str = unsafe {
        let type_str_ptr = cypher_astnode_typestr(node_type);
        if type_str_ptr.is_null() {
            format!("Unknown({node_type})")
        } else {
            CStr::from_ptr(type_str_ptr.cast::<c_char>())
                .to_string_lossy()
                .to_string()
        }
    };

    let n_children = unsafe { cypher_astnode_nchildren(node) };

    println!(
        "{}{} ({} children)",
        "  ".repeat(indent),
        type_str,
        n_children
    );

    // Print children recursively
    for i in 0..n_children {
        let child = unsafe { cypher_astnode_get_child(node, i) };
        print_ast(child, indent + 1);
    }
}

/// Prints the AST structure starting from a parse result
#[allow(dead_code)]
pub fn print_ast_from_result(result: *const cypher_parse_result_t) {
    if result.is_null() {
        println!("Parse result is null");
        return;
    }

    let n_roots = unsafe { cypher_parse_result_nroots(result) };
    println!("=== AST Structure ({n_roots} root nodes) ===");

    for i in 0..n_roots {
        let root = unsafe { cypher_parse_result_get_root(result, i) };
        if !root.is_null() {
            println!("\n--- Root Node {i} ---");
            print_ast(root, 0);
        }
    }
}

/// Enhanced AST printer with better formatting and type information
#[allow(dead_code)]
pub fn print_ast_enhanced(node: *const cypher_astnode_t, indent: usize) {
    if node.is_null() {
        println!("{}<null>", "  ".repeat(indent));
        return;
    }

    let node_type = unsafe { cypher_astnode_type(node) };
    let type_str = unsafe {
        let type_str_ptr = cypher_astnode_typestr(node_type);
        if type_str_ptr.is_null() {
            format!("Unknown({node_type})")
        } else {
            CStr::from_ptr(type_str_ptr.cast::<c_char>())
                .to_string_lossy()
                .to_string()
        }
    };
    let n_children = unsafe { cypher_astnode_nchildren(node) };

    // Print node information
    println!(
        "{}{} (type: {}, children: {})",
        "  ".repeat(indent),
        type_str,
        node_type,
        n_children
    );

    // Print children recursively
    for i in 0..n_children {
        let child = unsafe { cypher_astnode_get_child(node, i) };
        print_ast_enhanced(child, indent + 1);
    }
}

/// Enhanced AST printer starting from parse result
#[allow(dead_code)]
pub fn print_ast_from_result_enhanced(result: *const cypher_parse_result_t) {
    if result.is_null() {
        println!("Parse result is null");
        return;
    }

    let n_roots = unsafe { cypher_parse_result_nroots(result) };
    println!("=== Enhanced AST Structure ({n_roots} root nodes) ===");

    for i in 0..n_roots {
        let root = unsafe { cypher_parse_result_get_root(result, i) };
        if !root.is_null() {
            println!("\n--- Root Node {i} ---");
            print_ast_enhanced(root, 0);
        }
    }
}

/// Represents a return projection in a RETURN clause
#[derive(Debug, Clone, PartialEq)]
pub struct ReturnProjection {
    /// The expression being projected
    pub expression: String,
    /// Optional alias (AS clause)
    pub alias: Option<String>,
    /// Whether this projection is an identifier (for * expansion logic)
    pub is_identifier: bool,
}

/// Represents a RETURN clause
#[derive(Debug, Clone, PartialEq)]
pub struct ReturnClause {
    /// Whether DISTINCT is specified
    pub distinct: bool,
    /// Whether * is used (return all exported variables)
    pub return_star: bool,
    /// List of specific projections (empty if return_star is true)
    pub projections: Vec<ReturnProjection>,
}

/// Finds both MATCH and RETURN clauses in a query and returns them as a tuple
pub fn find_match_and_return_clauses(
    result: *const cypher_parse_result_t,
) -> Option<(*const cypher_astnode_t, *const cypher_astnode_t)> {
    if result.is_null() {
        return None;
    }

    let n_roots = unsafe { cypher_parse_result_nroots(result) };
    if n_roots == 0 {
        return None;
    }

    // Get the first root (should be a QUERY node)
    let root = unsafe { cypher_parse_result_get_root(result, 0) };
    if root.is_null() {
        return None;
    }

    // Navigate to find MATCH and RETURN clauses
    find_match_and_return_from_node(root)
}

/// Searches for a query with exactly two clauses: MATCH followed by RETURN
/// Returns both MATCH and RETURN clauses as a tuple
pub fn find_match_and_return_from_node(
    node: *const cypher_astnode_t,
) -> Option<(*const cypher_astnode_t, *const cypher_astnode_t)> {
    if node.is_null() {
        return None;
    }

    let node_type = unsafe { cypher_astnode_type(node) };
    let n_children = unsafe { cypher_astnode_nchildren(node) };

    // Check if this is a query with clauses
    let cypher_query = unsafe { CYPHER_AST_QUERY };
    let cypher_match = unsafe { CYPHER_AST_MATCH };
    let cypher_return = unsafe { CYPHER_AST_RETURN };

    if node_type == cypher_query {
        // We only accept queries with exactly 2 clauses
        if n_children != 2 {
            return None;
        }

        // Check that first clause is MATCH
        let first_child = unsafe { cypher_astnode_get_child(node, 0) };
        if first_child.is_null() {
            return None;
        }
        let first_child_type = unsafe { cypher_astnode_type(first_child) };
        if first_child_type != cypher_match {
            return None;
        }

        // Check that second clause is RETURN
        let second_child = unsafe { cypher_astnode_get_child(node, 1) };
        if second_child.is_null() {
            return None;
        }
        let second_child_type = unsafe { cypher_astnode_type(second_child) };
        if second_child_type != cypher_return {
            return None;
        }

        // Both conditions met - return both clauses
        return Some((first_child, second_child));
    }

    // Recursively search children for query nodes
    for i in 0..n_children {
        let child = unsafe { cypher_astnode_get_child(node, i) };
        if let Some(clauses) = find_match_and_return_from_node(child) {
            return Some(clauses);
        }
    }

    None
}

// Declare external functions for RETURN clause parsing
unsafe extern "C" {
    fn cypher_ast_return_is_distinct(node: *const cypher_astnode_t) -> bool;
    fn cypher_ast_return_has_include_existing(node: *const cypher_astnode_t) -> bool;
    fn cypher_ast_return_nprojections(node: *const cypher_astnode_t) -> u32;
    fn cypher_ast_return_get_projection(
        node: *const cypher_astnode_t,
        index: u32,
    ) -> *const cypher_astnode_t;
    fn cypher_ast_projection_get_expression(
        node: *const cypher_astnode_t,
    ) -> *const cypher_astnode_t;
    fn cypher_ast_projection_get_alias(node: *const cypher_astnode_t) -> *const cypher_astnode_t;
}

/// Parses a RETURN clause AST node and extracts return projections
pub fn parse_return_clause(
    return_node: *const cypher_astnode_t,
) -> Result<ReturnClause, GraphError> {
    if return_node.is_null() {
        return Err(GraphError::InvalidAstNode);
    }

    let node_type = unsafe { cypher_astnode_type(return_node) };
    let cypher_return = unsafe { CYPHER_AST_RETURN };
    if node_type != cypher_return {
        return Err(GraphError::InvalidAstNode);
    }

    // Use libcypher-parser functions to get RETURN clause properties
    let distinct = unsafe { cypher_ast_return_is_distinct(return_node) };
    let return_star = unsafe { cypher_ast_return_has_include_existing(return_node) };
    let n_projections = unsafe { cypher_ast_return_nprojections(return_node) };

    let mut projections = Vec::new();

    // Parse projections only if not using RETURN *
    if !return_star {
        for i in 0..n_projections {
            let projection_node = unsafe { cypher_ast_return_get_projection(return_node, i) };
            if projection_node.is_null() {
                continue;
            }

            match parse_return_projection(projection_node) {
                Ok(projection) => projections.push(projection),
                Err(_) => continue, // Skip invalid projections
            }
        }
    }

    let return_clause = ReturnClause {
        distinct,
        return_star,
        projections,
    };

    Ok(return_clause)
}

/// Parses a return projection (individual item in RETURN clause)
fn parse_return_projection(
    projection_node: *const cypher_astnode_t,
) -> Result<ReturnProjection, GraphError> {
    if projection_node.is_null() {
        return Err(GraphError::InvalidAstNode);
    }

    let node_type = unsafe { cypher_astnode_type(projection_node) };
    let cypher_projection = unsafe { CYPHER_AST_PROJECTION };
    if node_type != cypher_projection {
        return Err(GraphError::InvalidAstNode);
    }

    // Get the expression part of the projection
    let expression_node = unsafe { cypher_ast_projection_get_expression(projection_node) };
    if expression_node.is_null() {
        return Err(GraphError::InvalidAstNode);
    }

    // Extract the expression as a string (simplified - in practice this would be more complex)
    let expression = extract_expression_as_string(expression_node)?;

    // Check if the expression is just an identifier
    let is_identifier = {
        let expr_type = unsafe { cypher_astnode_type(expression_node) };
        let cypher_identifier = unsafe { CYPHER_AST_IDENTIFIER };
        expr_type == cypher_identifier
    };

    // Get the alias (AS clause) if present
    let alias_node = unsafe { cypher_ast_projection_get_alias(projection_node) };
    let alias = if !alias_node.is_null() {
        extract_identifier(alias_node)
    } else {
        None
    };

    Ok(ReturnProjection {
        expression,
        alias,
        is_identifier,
    })
}

/// Extracts an expression as a string representation (simplified implementation)
fn extract_expression_as_string(expr_node: *const cypher_astnode_t) -> Result<String, GraphError> {
    if expr_node.is_null() {
        return Err(GraphError::InvalidAstNode);
    }

    let node_type = unsafe { cypher_astnode_type(expr_node) };
    let cypher_identifier = unsafe { CYPHER_AST_IDENTIFIER };

    // For now, we'll only handle identifier expressions
    // A full implementation would handle property access, function calls, etc.
    if node_type == cypher_identifier {
        if let Some(identifier) = extract_identifier(expr_node) {
            return Ok(identifier);
        }
    }

    // For non-identifier expressions, return a placeholder
    // In a full implementation, this would recursively build the expression string
    Ok("?expr".to_string())
}

#[cfg(test)]
#[path = "tests_cypher.rs"]
mod tests;
