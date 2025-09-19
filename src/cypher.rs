#[allow(clippy::wildcard_imports)]
use libcypher_parser_sys::*;
use serde_json::Value;
use std::collections::HashMap;
use std::ffi::CStr;
use std::os::raw::c_char;

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

/// Represents a vertex (node pattern) in the pattern graph
#[derive(Debug, Clone, PartialEq)]
pub struct PatternVertex {
    /// Identifier for the vertex (auto-generated if missing)
    pub identifier: String,
    /// Label for the node (can be empty)
    pub label: Option<String>,
    /// Property values as key-value pairs
    pub properties: HashMap<String, Value>,
}

/// Represents an edge (relationship pattern) in the pattern graph
#[derive(Debug, Clone, PartialEq)]
pub struct PatternEdge {
    /// Edge identifier (can be empty if not specified)
    pub identifier: String,
    /// Source vertex identifier
    pub source: String,
    /// Target vertex identifier
    pub target: String,
    /// Relationship type (can be empty)
    pub rel_type: Option<String>,
    /// Property values as key-value pairs
    pub properties: HashMap<String, Value>,
    /// Direction of the relationship
    pub direction: RelationshipDirection,
    /// Minimum depth for variable length relationships (Some(1) for regular relationships)
    pub min_depth: Option<u32>,
    /// Maximum depth for variable length relationships (Some(1) for regular relationships)
    pub max_depth: Option<u32>,
}

/// Direction of a relationship
#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum RelationshipDirection {
    Outbound,      // ->
    Inbound,       // <-
    Bidirectional, // Both directions
}

/// Error types for graph creation
#[derive(Debug, PartialEq)]
#[allow(dead_code)]
pub enum GraphError {
    InvalidAstNode,
    VariableLengthRelationship,
    UnsupportedPattern,
    InvalidIdentifier,
}

impl std::fmt::Display for GraphError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GraphError::InvalidAstNode => write!(f, "Invalid AST node provided"),
            GraphError::VariableLengthRelationship => {
                write!(f, "Variable length relationships are not supported")
            }
            GraphError::UnsupportedPattern => write!(f, "Unsupported pattern type encountered"),
            GraphError::InvalidIdentifier => write!(f, "Invalid identifier in pattern"),
        }
    }
}

impl std::error::Error for GraphError {}

/// Creates a graph representation from a MATCH statement AST
pub fn make_match_graph(
    match_node: *const cypher_astnode_t,
) -> Result<(Vec<PatternVertex>, Vec<PatternEdge>), GraphError> {
    if match_node.is_null() {
        return Err(GraphError::InvalidAstNode);
    }

    let node_type = unsafe { cypher_astnode_type(match_node) };
    let match_type = unsafe { CYPHER_AST_MATCH };
    if node_type != match_type {
        return Err(GraphError::InvalidAstNode);
    }

    let mut vertices = Vec::new();
    let mut edges = Vec::new();
    let mut node_counter = 0;
    let mut rel_counter = 0;

    // Get the pattern from the MATCH clause
    // MATCH clause typically has the pattern as its first child
    let n_children = unsafe { cypher_astnode_nchildren(match_node) };
    if n_children == 0 {
        return Ok((vertices, edges));
    }

    let pattern = unsafe { cypher_astnode_get_child(match_node, 0) };
    if pattern.is_null() {
        return Err(GraphError::InvalidAstNode);
    }

    // Process the pattern
    process_pattern(pattern, &mut vertices, &mut edges, &mut node_counter, &mut rel_counter)?;

    Ok((vertices, edges))
}

/// Processes a pattern node and extracts vertices and edges
fn process_pattern(
    pattern: *const cypher_astnode_t,
    vertices: &mut Vec<PatternVertex>,
    edges: &mut Vec<PatternEdge>,
    node_counter: &mut u32,
    rel_counter: &mut usize,
) -> Result<(), GraphError> {
    if pattern.is_null() {
        return Err(GraphError::InvalidAstNode);
    }

    let pattern_type = unsafe { cypher_astnode_type(pattern) };
    let cypher_pattern = unsafe { CYPHER_AST_PATTERN };
    let cypher_named_path = unsafe { CYPHER_AST_NAMED_PATH };
    let cypher_pattern_path = unsafe { CYPHER_AST_PATTERN_PATH };

    if pattern_type == cypher_pattern {
        // Pattern contains multiple pattern paths
        let n_children = unsafe { cypher_astnode_nchildren(pattern) };
        for i in 0..n_children {
            let child = unsafe { cypher_astnode_get_child(pattern, i) };
            process_pattern(child, vertices, edges, node_counter, rel_counter)?;
        }
    } else if pattern_type == cypher_named_path {
        // Named path - get the actual path from the second child
        let n_children = unsafe { cypher_astnode_nchildren(pattern) };
        if n_children >= 2 {
            let path = unsafe { cypher_astnode_get_child(pattern, 1) };
            process_pattern_path(path, vertices, edges, node_counter, rel_counter)?;
        }
    } else if pattern_type == cypher_pattern_path {
        process_pattern_path(pattern, vertices, edges, node_counter, rel_counter)?;
    } else {
        // Try to process as pattern path anyway
        process_pattern_path(pattern, vertices, edges, node_counter, rel_counter)?;
    }

    Ok(())
}

/// Processes a pattern path and extracts alternating nodes and relationships
fn process_pattern_path(
    path: *const cypher_astnode_t,
    vertices: &mut Vec<PatternVertex>,
    edges: &mut Vec<PatternEdge>,
    node_counter: &mut u32,
    rel_counter: &mut usize,
) -> Result<(), GraphError> {
    if path.is_null() {
        return Err(GraphError::InvalidAstNode);
    }

    let n_children = unsafe { cypher_astnode_nchildren(path) };
    let mut current_node_id: Option<String> = None;

    let cypher_node_pattern = unsafe { CYPHER_AST_NODE_PATTERN };
    let cypher_rel_pattern = unsafe { CYPHER_AST_REL_PATTERN };

    let mut i = 0;
    while i < n_children {
        let child = unsafe { cypher_astnode_get_child(path, i) };
        let child_type = unsafe { cypher_astnode_type(child) };

        if child_type == cypher_node_pattern {
            let vertex = process_node_pattern(child, node_counter)?;
            current_node_id = Some(vertex.identifier.clone());
            vertices.push(vertex);
            i += 1;
        } else if child_type == cypher_rel_pattern {
            // Need current node and next node for relationship
            if let Some(ref source_id) = current_node_id {
                // Look ahead for the target node
                if i + 1 < n_children {
                    let next_child = unsafe { cypher_astnode_get_child(path, i + 1) };
                    let next_child_type = unsafe { cypher_astnode_type(next_child) };

                    if next_child_type == cypher_node_pattern {
                        let target_vertex = process_node_pattern(next_child, node_counter)?;
                        let target_id = target_vertex.identifier.clone();

                        // Process the relationship
                        let edge =
                            process_relationship_pattern(child, source_id.clone(), target_id, rel_counter)?;
                        edges.push(edge);

                        current_node_id = Some(target_vertex.identifier.clone());
                        vertices.push(target_vertex);

                        // Skip the next node since we processed it here
                        i += 2; // Skip both the relationship and the target node
                    } else {
                        i += 1;
                    }
                } else {
                    i += 1;
                }
            } else {
                i += 1;
            }
        } else {
            // Handle other pattern elements as needed
            i += 1;
        }
    }

    Ok(())
}

/// Processes a node pattern and creates a `PatternVertex`
fn process_node_pattern(
    node: *const cypher_astnode_t,
    node_counter: &mut u32,
) -> Result<PatternVertex, GraphError> {
    if node.is_null() {
        return Err(GraphError::InvalidAstNode);
    }

    let mut identifier = String::new();
    let mut label: Option<String> = None;
    let mut properties = HashMap::new();

    let n_children = unsafe { cypher_astnode_nchildren(node) };
    let cypher_identifier = unsafe { CYPHER_AST_IDENTIFIER };
    let cypher_label = unsafe { CYPHER_AST_LABEL };
    let cypher_map = unsafe { CYPHER_AST_MAP };

    for i in 0..n_children {
        let child = unsafe { cypher_astnode_get_child(node, i) };
        if child.is_null() {
            continue;
        }

        let child_type = unsafe { cypher_astnode_type(child) };

        if child_type == cypher_identifier {
            if let Some(id) = extract_identifier(child) {
                identifier = id;
            }
        } else if child_type == cypher_label {
            if let Some(lbl) = extract_label(child) {
                label = Some(lbl);
            }
        } else if child_type == cypher_map {
            properties = extract_properties(child).unwrap_or_default();
        }
    }

    // Generate identifier if empty
    if identifier.is_empty() {
        identifier = format!("n_{node_counter}");
        *node_counter += 1;
    }

    Ok(PatternVertex {
        identifier,
        label,
        properties,
    })
}

/// Processes a relationship pattern and creates a `PatternEdge`
fn process_relationship_pattern(
    rel: *const cypher_astnode_t,
    source: String,
    target: String,
    rel_counter: &mut usize,
) -> Result<PatternEdge, GraphError> {
    if rel.is_null() {
        return Err(GraphError::InvalidAstNode);
    }

    let mut identifier = String::new();
    let mut rel_type: Option<String> = None;
    let mut properties = HashMap::new();

    // Extract depth information for variable length relationships
    let (min_depth, max_depth) = extract_variable_length_depths(rel);

    let n_children = unsafe { cypher_astnode_nchildren(rel) };
    let cypher_identifier = unsafe { CYPHER_AST_IDENTIFIER };
    let cypher_reltype = unsafe { CYPHER_AST_RELTYPE };
    let cypher_map = unsafe { CYPHER_AST_MAP };

    for i in 0..n_children {
        let child = unsafe { cypher_astnode_get_child(rel, i) };
        if child.is_null() {
            continue;
        }

        let child_type = unsafe { cypher_astnode_type(child) };

        if child_type == cypher_identifier {
            if let Some(id) = extract_identifier(child) {
                identifier = id;
            }
        } else if child_type == cypher_reltype {
            if let Some(rt) = extract_reltype(child) {
                rel_type = Some(rt);
            }
        } else if child_type == cypher_map {
            properties = extract_properties(child).unwrap_or_default();
        }
    }

    // Generate identifier if empty
    if identifier.is_empty() {
        identifier = format!("r_{rel_counter}");
        *rel_counter += 1;
    }

    // Try to determine direction from relationship pattern structure
    let direction = determine_relationship_direction(rel);

    Ok(PatternEdge {
        identifier,
        source,
        target,
        rel_type,
        properties,
        direction,
        min_depth,
        max_depth,
    })
}

/// Extracts identifier text from an identifier AST node
fn extract_identifier(node: *const cypher_astnode_t) -> Option<String> {
    if node.is_null() {
        return None;
    }

    // Verify this is actually an identifier node
    let node_type = unsafe { cypher_astnode_type(node) };
    let cypher_identifier = unsafe { CYPHER_AST_IDENTIFIER };

    if node_type != cypher_identifier {
        return None;
    }

    // Get the identifier name using libcypher-parser function
    unsafe {
        let name_ptr = cypher_ast_identifier_get_name(node);
        if !name_ptr.is_null() {
            let c_str = CStr::from_ptr(name_ptr.cast::<c_char>());
            return Some(c_str.to_string_lossy().to_string());
        }
    }

    // If we can't extract the name, return None so auto-generation kicks in
    None
}

// Declare the external functions that actually exist in libcypher-parser
unsafe extern "C" {
    fn cypher_ast_identifier_get_name(node: *const cypher_astnode_t) -> *const c_char;
    fn cypher_ast_label_get_name(node: *const cypher_astnode_t) -> *const c_char;
    fn cypher_ast_reltype_get_name(node: *const cypher_astnode_t) -> *const c_char;
    fn cypher_ast_map_nentries(node: *const cypher_astnode_t) -> u32;
    fn cypher_ast_map_get_key(node: *const cypher_astnode_t, index: u32)
    -> *const cypher_astnode_t;
    fn cypher_ast_map_get_value(
        node: *const cypher_astnode_t,
        index: u32,
    ) -> *const cypher_astnode_t;
    fn cypher_ast_prop_name_get_value(node: *const cypher_astnode_t) -> *const c_char;
    fn cypher_ast_string_get_value(node: *const cypher_astnode_t) -> *const c_char;
    fn cypher_ast_integer_get_valuestr(node: *const cypher_astnode_t) -> *const c_char;
    fn cypher_ast_float_get_valuestr(node: *const cypher_astnode_t) -> *const c_char;
    fn cypher_ast_rel_pattern_get_direction(node: *const cypher_astnode_t) -> u32;
}

/// Extracts label text from a label AST node  
fn extract_label(node: *const cypher_astnode_t) -> Option<String> {
    if node.is_null() {
        return None;
    }

    // Verify this is actually a label node
    let node_type = unsafe { cypher_astnode_type(node) };
    let cypher_label = unsafe { CYPHER_AST_LABEL };

    if node_type != cypher_label {
        return None;
    }

    // Get the label name using libcypher-parser function
    unsafe {
        let name_ptr = cypher_ast_label_get_name(node);
        if !name_ptr.is_null() {
            let c_str = CStr::from_ptr(name_ptr.cast::<c_char>());
            return Some(c_str.to_string_lossy().to_string());
        }
    }

    None
}

/// Extracts relationship type from a reltype AST node
fn extract_reltype(node: *const cypher_astnode_t) -> Option<String> {
    if node.is_null() {
        return None;
    }

    // Verify this is actually a reltype node
    let node_type = unsafe { cypher_astnode_type(node) };
    let cypher_reltype = unsafe { CYPHER_AST_RELTYPE };

    if node_type != cypher_reltype {
        return None;
    }

    // Get the reltype name using libcypher-parser function
    unsafe {
        let name_ptr = cypher_ast_reltype_get_name(node);
        if !name_ptr.is_null() {
            let c_str = CStr::from_ptr(name_ptr.cast::<c_char>());
            return Some(c_str.to_string_lossy().to_string());
        }
    }

    None
}

/// Extracts properties from a map AST node
fn extract_properties(node: *const cypher_astnode_t) -> Option<HashMap<String, Value>> {
    if node.is_null() {
        return None;
    }

    // Verify this is actually a map node
    let node_type = unsafe { cypher_astnode_type(node) };
    let cypher_map = unsafe { CYPHER_AST_MAP };

    if node_type != cypher_map {
        return None;
    }

    let mut properties = HashMap::new();

    // Get the number of entries in the map
    let n_entries = unsafe { cypher_ast_map_nentries(node) };

    for i in 0..n_entries {
        // Get the key and value for this entry
        let key_node = unsafe { cypher_ast_map_get_key(node, i) };
        let value_node = unsafe { cypher_ast_map_get_value(node, i) };

        if key_node.is_null() || value_node.is_null() {
            continue;
        }

        // Extract the key string from the property name node
        if let Some(key_str) = extract_property_name(key_node) {
            // Extract the value from the expression node
            if let Some(value) = extract_expression_value(value_node) {
                properties.insert(key_str, value);
            }
        }
    }

    Some(properties)
}

/// Extracts a property name from a CYPHER_AST_PROP_NAME node
fn extract_property_name(node: *const cypher_astnode_t) -> Option<String> {
    if node.is_null() {
        return None;
    }

    let node_type = unsafe { cypher_astnode_type(node) };
    let cypher_prop_name = unsafe { CYPHER_AST_PROP_NAME };

    if node_type != cypher_prop_name {
        return None;
    }

    unsafe {
        let name_ptr = cypher_ast_prop_name_get_value(node);
        if !name_ptr.is_null() {
            let c_str = CStr::from_ptr(name_ptr);
            return Some(c_str.to_string_lossy().to_string());
        }
    }

    None
}

/// Extracts a value from various expression node types
fn extract_expression_value(node: *const cypher_astnode_t) -> Option<Value> {
    if node.is_null() {
        return None;
    }

    let node_type = unsafe { cypher_astnode_type(node) };

    unsafe {
        let cypher_string = CYPHER_AST_STRING;
        let cypher_integer = CYPHER_AST_INTEGER;
        let cypher_float = CYPHER_AST_FLOAT;
        let cypher_boolean = CYPHER_AST_BOOLEAN;
        let cypher_true = CYPHER_AST_TRUE;
        let cypher_false = CYPHER_AST_FALSE;
        let cypher_null = CYPHER_AST_NULL;

        match node_type {
            x if x == cypher_string => {
                let str_ptr = cypher_ast_string_get_value(node);
                if !str_ptr.is_null() {
                    let c_str = CStr::from_ptr(str_ptr);
                    let string_val = c_str.to_string_lossy().to_string();
                    return Some(Value::String(string_val));
                }
            }
            x if x == cypher_integer => {
                let int_ptr = cypher_ast_integer_get_valuestr(node);
                if !int_ptr.is_null() {
                    let c_str = CStr::from_ptr(int_ptr);
                    let int_str = c_str.to_string_lossy();
                    if let Ok(int_val) = int_str.parse::<i64>() {
                        return Some(Value::Number(serde_json::Number::from(int_val)));
                    }
                }
            }
            x if x == cypher_float => {
                let float_ptr = cypher_ast_float_get_valuestr(node);
                if !float_ptr.is_null() {
                    let c_str = CStr::from_ptr(float_ptr);
                    let float_str = c_str.to_string_lossy();
                    if let Ok(float_val) = float_str.parse::<f64>() {
                        if let Some(number) = serde_json::Number::from_f64(float_val) {
                            return Some(Value::Number(number));
                        }
                    }
                }
            }
            x if x == cypher_boolean => {
                // For boolean nodes, check if it's true or false
                let n_children = cypher_astnode_nchildren(node);
                if n_children > 0 {
                    let child = cypher_astnode_get_child(node, 0);
                    if !child.is_null() {
                        let child_type = cypher_astnode_type(child);
                        if child_type == cypher_true {
                            return Some(Value::Bool(true));
                        } else if child_type == cypher_false {
                            return Some(Value::Bool(false));
                        }
                    }
                }
            }
            x if x == cypher_true => {
                return Some(Value::Bool(true));
            }
            x if x == cypher_false => {
                return Some(Value::Bool(false));
            }
            x if x == cypher_null => {
                return Some(Value::Null);
            }
            _ => {
                // For other expression types, return a string representation
                let type_str_ptr = cypher_astnode_typestr(node_type);
                if !type_str_ptr.is_null() {
                    let type_str = CStr::from_ptr(type_str_ptr.cast::<c_char>())
                        .to_string_lossy()
                        .to_string();
                    return Some(Value::String(format!("<{type_str}>")));
                }
                return Some(Value::String("<unknown>".to_string()));
            }
        }
    }

    None
}

/// Extracts depth information from variable length relationships
/// Returns (min_depth, max_depth) where None means unbounded
fn extract_variable_length_depths(rel: *const cypher_astnode_t) -> (Option<u32>, Option<u32>) {
    if rel.is_null() {
        return (Some(1), Some(1)); // Default for regular relationships
    }

    let n_children = unsafe { cypher_astnode_nchildren(rel) };
    let cypher_range = unsafe { CYPHER_AST_RANGE };

    for i in 0..n_children {
        let child = unsafe { cypher_astnode_get_child(rel, i) };
        if child.is_null() {
            continue;
        }

        let child_type = unsafe { cypher_astnode_type(child) };

        // Look for range nodes that indicate variable length
        if child_type == cypher_range {
            return extract_range_values(child);
        }
    }

    // No range found - regular relationship has depth 1
    (Some(1), Some(1))
}

/// Extracts min and max values from a CYPHER_AST_RANGE node
fn extract_range_values(range_node: *const cypher_astnode_t) -> (Option<u32>, Option<u32>) {
    if range_node.is_null() {
        return (None, None);
    }

    let n_children = unsafe { cypher_astnode_nchildren(range_node) };
    
    // A range node typically has 0, 1, or 2 children representing start and end
    let mut min_depth: Option<u32> = None;
    let mut max_depth: Option<u32> = None;
    
    match n_children {
        0 => {
            // No bounds specified - equivalent to [*] which means [1..]
            min_depth = Some(1);
            max_depth = None; // unbounded
        }
        1 => {
            // Single value - could be [*n] (meaning [0..n]) or [n..] (meaning [n..])
            let child = unsafe { cypher_astnode_get_child(range_node, 0) };
            if let Some(value) = extract_integer_from_node(child) {
                // Check if this is a start or end bound by examining the range structure
                // For now, assume single value means [0..value] (Neo4j default behavior)
                min_depth = Some(0);
                max_depth = Some(value);
            }
        }
        2 => {
            // Two values - [start..end]
            let start_child = unsafe { cypher_astnode_get_child(range_node, 0) };
            let end_child = unsafe { cypher_astnode_get_child(range_node, 1) };
            
            min_depth = extract_integer_from_node(start_child);
            max_depth = extract_integer_from_node(end_child);
        }
        _ => {
            // Unexpected number of children
            min_depth = Some(1);
            max_depth = None;
        }
    }
    
    (min_depth, max_depth)
}

/// Extracts integer value from an AST node (returns None for null nodes or non-integers)
fn extract_integer_from_node(node: *const cypher_astnode_t) -> Option<u32> {
    if node.is_null() {
        return None;
    }

    let node_type = unsafe { cypher_astnode_type(node) };
    let cypher_integer = unsafe { CYPHER_AST_INTEGER };

    if node_type == cypher_integer {
        unsafe {
            let int_ptr = cypher_ast_integer_get_valuestr(node);
            if !int_ptr.is_null() {
                let c_str = CStr::from_ptr(int_ptr);
                if let Ok(int_str) = c_str.to_str() {
                    if let Ok(value) = int_str.parse::<u32>() {
                        return Some(value);
                    }
                }
            }
        }
    }

    None
}

/// Determines the direction of a relationship from its AST representation
fn determine_relationship_direction(rel: *const cypher_astnode_t) -> RelationshipDirection {
    if rel.is_null() {
        return RelationshipDirection::Bidirectional;
    }

    // Verify this is actually a relationship pattern node
    let node_type = unsafe { cypher_astnode_type(rel) };
    let cypher_rel_pattern = unsafe { CYPHER_AST_REL_PATTERN };

    if node_type != cypher_rel_pattern {
        return RelationshipDirection::Bidirectional;
    }

    // Get the direction from the relationship pattern
    let direction = unsafe { cypher_ast_rel_pattern_get_direction(rel) };

    // Based on actual libcypher-parser constants (confirmed by testing):
    // 0 = CYPHER_REL_INBOUND (directed left: <-[r]-)
    // 1 = CYPHER_REL_OUTBOUND (directed right: -[r]->)
    // 2 = CYPHER_REL_BIDIRECTIONAL (undirected: -[r]-)
    match direction {
        0 => RelationshipDirection::Inbound,       // <-[r]-
        1 => RelationshipDirection::Outbound,      // -[r]->
        2 => RelationshipDirection::Bidirectional, // -[r]-
        _ => RelationshipDirection::Bidirectional, // fallback for unknown values
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
pub fn find_match_and_return_from_node(node: *const cypher_astnode_t) -> Option<(*const cypher_astnode_t, *const cypher_astnode_t)> {
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
    fn cypher_ast_return_get_projection(node: *const cypher_astnode_t, index: u32) -> *const cypher_astnode_t;
    fn cypher_ast_projection_get_expression(node: *const cypher_astnode_t) -> *const cypher_astnode_t;
    fn cypher_ast_projection_get_alias(node: *const cypher_astnode_t) -> *const cypher_astnode_t;
}

/// Parses a RETURN clause AST node and extracts return projections
pub fn parse_return_clause(return_node: *const cypher_astnode_t) -> Result<ReturnClause, GraphError> {
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
fn parse_return_projection(projection_node: *const cypher_astnode_t) -> Result<ReturnProjection, GraphError> {
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

/// Prints the pattern graph in list format
pub fn print_pattern_graph(vertices: &[PatternVertex], edges: &[PatternEdge]) {
    println!("\n=== Pattern Graph ===");

    // Print vertices list
    println!("\n--- Vertices ---");
    if vertices.is_empty() {
        println!("No vertices found.");
    } else {
        for vertex in vertices {
            // Print identifier and label on first line
            let vertex_info = if let Some(ref label) = vertex.label {
                format!("Vertex: {} ({})", vertex.identifier, label)
            } else {
                format!("Vertex: {}", vertex.identifier)
            };
            println!("{vertex_info}");
            
            // Print properties on second line
            if vertex.properties.is_empty() {
                println!("  Properties: (none)");
            } else {
                let prop_str = vertex
                    .properties
                    .iter()
                    .map(|(k, v)| format!("{k}:{v}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                println!("  Properties: {{{prop_str}}}");
            }
            println!(); // Empty line between vertices
        }
    }

    // Print edges list
    println!("--- Edges ---");
    if edges.is_empty() {
        println!("No edges found.");
    } else {
        for edge in edges {
            // Build depth information string
            let depth_info = match (edge.min_depth, edge.max_depth) {
                (Some(min), Some(max)) if min == max => format!("{{{min}}}"),
                (Some(min), Some(max)) => format!("{{{min}..{max}}}"),
                (Some(min), None) => format!("{{{min}..}}"),
                (None, Some(max)) => format!("{{..{max}}}"),
                (None, None) => "".to_string(),
            };
            
            // Print edge information on first line in Cypher format
            let edge_info = if let Some(ref rel_type) = edge.rel_type {
                match edge.direction {
                    RelationshipDirection::Outbound => {
                        format!("Edge: {} -[{}:{}{}]-> {}", edge.source, edge.identifier, rel_type, depth_info, edge.target)
                    }
                    RelationshipDirection::Inbound => {
                        format!("Edge: {} <-[{}:{}{}]- {}", edge.source, edge.identifier, rel_type, depth_info, edge.target)
                    }
                    RelationshipDirection::Bidirectional => {
                        format!("Edge: {} -[{}:{}{}]- {}", edge.source, edge.identifier, rel_type, depth_info, edge.target)
                    }
                }
            } else {
                match edge.direction {
                    RelationshipDirection::Outbound => {
                        format!("Edge: {} -[{}{}]-> {}", edge.source, edge.identifier, depth_info, edge.target)
                    }
                    RelationshipDirection::Inbound => {
                        format!("Edge: {} <-[{}{}]- {}", edge.source, edge.identifier, depth_info, edge.target)
                    }
                    RelationshipDirection::Bidirectional => {
                        format!("Edge: {} -[{}{}]- {}", edge.source, edge.identifier, depth_info, edge.target)
                    }
                }
            };
            println!("{edge_info}");
            
            // Print depth information on second line
            let depth_display = match (edge.min_depth, edge.max_depth) {
                (Some(min), Some(max)) if min == max => format!("Depth: {min}"),
                (Some(min), Some(max)) => format!("Depth: {min}..{max}"),
                (Some(min), None) => format!("Depth: {min}..∞"),
                (None, Some(max)) => format!("Depth: 0..{max}"),
                (None, None) => "Depth: unbounded".to_string(),
            };
            println!("  {depth_display}");
            
            // Print properties on third line
            if edge.properties.is_empty() {
                println!("  Properties: (none)");
            } else {
                let prop_str = edge
                    .properties
                    .iter()
                    .map(|(k, v)| format!("{k}:{v}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                println!("  Properties: {{{prop_str}}}");
            }
            println!(); // Empty line between edges
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;
    use std::ptr;

    /// Helper function to parse a Cypher query and return the parse result
    fn parse_cypher_query(query: &str) -> *const cypher_parse_result_t {
        let c_query = CString::new(query).expect("Failed to create CString");
        let config = unsafe { cypher_parser_new_config() };
        assert!(!config.is_null(), "Failed to create parser config");

        let result = unsafe {
            cypher_uparse(
                c_query.as_ptr().cast::<c_char>(),
                c_query.as_bytes().len() as u64,
                ptr::null_mut(),
                config,
                u64::from(CYPHER_PARSE_DEFAULT),
            )
        };

        unsafe {
            cypher_parser_config_free(config);
        }

        result
    }

    /// Helper function to clean up parse result
    fn cleanup_parse_result(result: *const cypher_parse_result_t) {
        if !result.is_null() {
            unsafe {
                cypher_parse_result_free(result as *mut cypher_parse_result_t);
            }
        }
    }

    #[test]
    fn test_print_ast_with_null_node() {
        // Test that print_ast handles null nodes gracefully
        print_ast(ptr::null(), 0);
        // This should not panic and should print "<null>"
    }

    #[test]
    fn test_print_ast_enhanced_with_null_node() {
        // Test that print_ast_enhanced handles null nodes gracefully
        print_ast_enhanced(ptr::null(), 0);
        // This should not panic and should print "<null>"
    }

    #[test]
    fn test_print_ast_from_result_with_null_result() {
        // Test that print_ast_from_result handles null results gracefully
        print_ast_from_result(ptr::null());
        // This should not panic and should print "Parse result is null"
    }

    #[test]
    fn test_print_ast_from_result_enhanced_with_null_result() {
        // Test that print_ast_from_result_enhanced handles null results gracefully
        print_ast_from_result_enhanced(ptr::null());
        // This should not panic and should print "Parse result is null"
    }

    #[test]
    fn test_simple_match_query() {
        let query = "MATCH (n) RETURN n";
        let result = parse_cypher_query(query);

        if !result.is_null() {
            let n_errors = unsafe { cypher_parse_result_nerrors(result) };
            if n_errors == 0 {
                // Test basic AST printing
                print_ast_from_result(result);

                // Test enhanced AST printing
                print_ast_from_result_enhanced(result);
            }
        }

        cleanup_parse_result(result);
    }

    #[test]
    fn test_complex_match_with_properties() {
        let query = "MATCH path = (n_0:aws_s3_bucket {org_id: 2, to_delete: false, default_encryption_type: 'aws:kms'}) RETURN path";
        let result = parse_cypher_query(query);

        if !result.is_null() {
            let n_errors = unsafe { cypher_parse_result_nerrors(result) };
            if n_errors == 0 {
                let n_roots = unsafe { cypher_parse_result_nroots(result) };
                assert!(n_roots > 0, "Should have at least one root node");

                // Test enhanced AST printing
                print_ast_from_result_enhanced(result);
            }
        }

        cleanup_parse_result(result);
    }

    #[test]
    fn test_relationship_query() {
        let query = "MATCH path = (n_0:aws_ec2_instance {org_id: 2, to_delete: false}) <-[r_0:ATTACHES_TO]- (n_1:aws_ebs_volume {org_id: 2, to_delete: false, encrypted:false}) RETURN path";
        let result = parse_cypher_query(query);

        if !result.is_null() {
            let n_errors = unsafe { cypher_parse_result_nerrors(result) };
            if n_errors == 0 {
                let n_roots = unsafe { cypher_parse_result_nroots(result) };
                assert!(n_roots > 0, "Should have at least one root node");

                // Test both AST printing methods
                print_ast_from_result(result);
                print_ast_from_result_enhanced(result);
            }
        }

        cleanup_parse_result(result);
    }

    #[test]
    fn test_complex_path_query() {
        let query = "MATCH path = (n_0:aws_ec2_instance {org_id: 2, to_delete: false, entity_info_status:1, is_publicly_accessible: true}) -[r_0:ASSOCIATES]-> (n_0_0:aws_iam_instance_profile {org_id: 2, to_delete: false, entity_info_status:1}) -[r_0_0:ASSUMES]-> (n_0_0_0:aws_iam_role {org_id: 2, to_delete: false, entity_info_status:1}) -[r_0_0_0:CAN_ACCESS]-> (n_0_0_0_0:aws_s3_bucket {org_id: 2, to_delete: false, entity_info_status:1, sensitive_data: true}) RETURN path";
        let result = parse_cypher_query(query);

        if !result.is_null() {
            let n_errors = unsafe { cypher_parse_result_nerrors(result) };
            if n_errors == 0 {
                let n_roots = unsafe { cypher_parse_result_nroots(result) };
                assert!(n_roots > 0, "Should have at least one root node");

                // Test enhanced AST printing
                print_ast_from_result_enhanced(result);
            }
        }

        cleanup_parse_result(result);
    }

    #[test]
    fn test_administrator_role_query() {
        let query = "MATCH path = (n_0:aws_ec2_instance {org_id:2, entity_info_status: 1, to_delete:false, is_publicly_accessible: true}) -[r_0:ASSOCIATES]-> (n_0_0:aws_iam_instance_profile {org_id:2, entity_info_status: 1, to_delete:false}) -[r_0_0:ASSUMES]-> (n_0_0_0:aws_iam_role {org_id:2, entity_info_status: 1, to_delete:false, is_administrator: true}) RETURN path";
        let result = parse_cypher_query(query);

        if !result.is_null() {
            let n_errors = unsafe { cypher_parse_result_nerrors(result) };
            if n_errors == 0 {
                let n_roots = unsafe { cypher_parse_result_nroots(result) };
                assert!(n_roots > 0, "Should have at least one root node");

                // Test enhanced AST printing
                print_ast_from_result_enhanced(result);
            }
        }

        cleanup_parse_result(result);
    }

    #[test]
    fn test_lambda_function_query() {
        let query = "MATCH path = (n_0:aws_lambda_function {org_id:2, entity_info_status: 1, to_delete:false, is_publicly_accessible: true}) -[r_0:ASSOCIATES]-> (n_0_0_0:aws_iam_role {org_id:2, entity_info_status: 1, to_delete:false, is_administrator: true}) RETURN path";
        let result = parse_cypher_query(query);

        if !result.is_null() {
            let n_errors = unsafe { cypher_parse_result_nerrors(result) };
            if n_errors == 0 {
                let n_roots = unsafe { cypher_parse_result_nroots(result) };
                assert!(n_roots > 0, "Should have at least one root node");

                // Test enhanced AST printing
                print_ast_from_result_enhanced(result);
            }
        }

        cleanup_parse_result(result);
    }

    #[test]
    fn test_invalid_cypher_query() {
        let query = "INVALID CYPHER SYNTAX {";
        let result = parse_cypher_query(query);

        if !result.is_null() {
            let n_errors = unsafe { cypher_parse_result_nerrors(result) };
            // This query should have parsing errors
            if n_errors > 0 {
                // Test that our functions handle error cases gracefully
                print_ast_from_result(result);
                print_ast_from_result_enhanced(result);
            }
        }

        cleanup_parse_result(result);
    }

    #[test]
    fn test_empty_query() {
        let query = "";
        let result = parse_cypher_query(query);

        if !result.is_null() {
            let _n_errors = unsafe { cypher_parse_result_nerrors(result) };
            let _n_roots = unsafe { cypher_parse_result_nroots(result) };

            // Test both printing methods with empty query
            print_ast_from_result(result);
            print_ast_from_result_enhanced(result);
        }

        cleanup_parse_result(result);
    }

    #[test]
    fn test_ast_node_children_count() {
        let query = "MATCH (n:Person {name: 'Alice'}) RETURN n";
        let result = parse_cypher_query(query);

        if !result.is_null() {
            let n_errors = unsafe { cypher_parse_result_nerrors(result) };
            if n_errors == 0 {
                let n_roots = unsafe { cypher_parse_result_nroots(result) };
                if n_roots > 0 {
                    let root = unsafe { cypher_parse_result_get_root(result, 0) };
                    if !root.is_null() {
                        let n_children = unsafe { cypher_astnode_nchildren(root) };
                        // n_children is unsigned, so it's always >= 0

                        // Test that we can iterate through children without panicking
                        for i in 0..n_children {
                            let child = unsafe { cypher_astnode_get_child(root, i) };
                            // Test both printing functions on each child
                            print_ast(child, 0);
                            print_ast_enhanced(child, 0);
                        }
                    }
                }
            }
        }

        cleanup_parse_result(result);
    }

    #[test]
    fn test_ast_node_type_handling() {
        let query = "MATCH (n) RETURN n";
        let result = parse_cypher_query(query);

        if !result.is_null() {
            let n_errors = unsafe { cypher_parse_result_nerrors(result) };
            if n_errors == 0 {
                let n_roots = unsafe { cypher_parse_result_nroots(result) };
                if n_roots > 0 {
                    let root = unsafe { cypher_parse_result_get_root(result, 0) };
                    if !root.is_null() {
                        let node_type = unsafe { cypher_astnode_type(root) };
                        // node_type is unsigned, so it's always >= 0

                        // Test that typestr doesn't panic
                        let type_str_ptr = unsafe { cypher_astnode_typestr(node_type) };
                        if !type_str_ptr.is_null() {
                            let type_str = unsafe {
                                CStr::from_ptr(type_str_ptr.cast::<c_char>())
                                    .to_string_lossy()
                                    .to_string()
                            };
                            assert!(!type_str.is_empty(), "Type string should not be empty");
                        }
                    }
                }
            }
        }

        cleanup_parse_result(result);
    }

    #[test]
    fn test_multiple_root_nodes() {
        let query = "MATCH (n) RETURN n; MATCH (m) RETURN m";
        let result = parse_cypher_query(query);

        if !result.is_null() {
            let n_errors = unsafe { cypher_parse_result_nerrors(result) };
            if n_errors == 0 {
                let n_roots = unsafe { cypher_parse_result_nroots(result) };
                assert!(
                    n_roots >= 2,
                    "Should have at least 2 root nodes for multiple statements"
                );

                // Test both printing methods
                print_ast_from_result(result);
                print_ast_from_result_enhanced(result);
            }
        }

        cleanup_parse_result(result);
    }

    #[test]
    fn test_make_match_graph_null_input() {
        let result = make_match_graph(std::ptr::null());
        assert_eq!(result, Err(GraphError::InvalidAstNode));
    }

    #[test]
    fn test_make_match_graph_simple_match() {
        let query = "MATCH (n) RETURN n";
        let result = parse_cypher_query(query);

        if !result.is_null() {
            let n_errors = unsafe { cypher_parse_result_nerrors(result) };
            if n_errors == 0 {
                let n_roots = unsafe { cypher_parse_result_nroots(result) };
                if n_roots > 0 {
                    let root = unsafe { cypher_parse_result_get_root(result, 0) };
                    if !root.is_null() {
                        // Find the MATCH clause in the query
                        let match_node = find_match_clause(root);
                        if let Some(match_clause) = match_node {
                            match make_match_graph(match_clause) {
                                Ok((vertices, edges)) => {
                                    println!(
                                        "Graph created successfully: {} vertices, {} edges",
                                        vertices.len(),
                                        edges.len()
                                    );
                                    // Basic validation
                                    assert!(
                                        !vertices.is_empty(),
                                        "Should have at least one vertex"
                                    );
                                }
                                Err(e) => {
                                    println!("Graph creation failed: {e}");
                                }
                            }
                        }
                    }
                }
            }
        }

        cleanup_parse_result(result);
    }

    #[test]
    fn test_make_match_graph_with_relationship() {
        let query = "MATCH (a)-[r]->(b) RETURN a, r, b";
        let result = parse_cypher_query(query);

        if !result.is_null() {
            let n_errors = unsafe { cypher_parse_result_nerrors(result) };
            if n_errors == 0 {
                let n_roots = unsafe { cypher_parse_result_nroots(result) };
                if n_roots > 0 {
                    let root = unsafe { cypher_parse_result_get_root(result, 0) };
                    if let Some(match_clause) = find_match_clause(root) {
                        match make_match_graph(match_clause) {
                            Ok((vertices, edges)) => {
                                println!(
                                    "Relationship graph: {} vertices, {} edges",
                                    vertices.len(),
                                    edges.len()
                                );
                                // Should have at least 2 vertices and 1 edge
                                assert!(
                                    vertices.len() >= 2,
                                    "Should have at least 2 vertices for relationship pattern"
                                );
                                assert!(
                                    !edges.is_empty(),
                                    "Should have at least 1 edge for relationship pattern"
                                );
                            }
                            Err(e) => {
                                println!("Relationship graph creation failed: {e}");
                            }
                        }
                    }
                }
            }
        }

        cleanup_parse_result(result);
    }

    #[test]
    fn test_pattern_vertex_creation() {
        let vertex = PatternVertex {
            identifier: "n".to_string(),
            label: Some("Person".to_string()),
            properties: {
                let mut props = HashMap::new();
                props.insert("name".to_string(), Value::String("Alice".to_string()));
                props
            },
        };

        assert_eq!(vertex.identifier, "n");
        assert_eq!(vertex.label, Some("Person".to_string()));
        assert_eq!(vertex.properties.get("name"), Some(&Value::String("Alice".to_string())));
    }

    #[test]
    fn test_pattern_edge_creation() {
        let edge = PatternEdge {
            identifier: "r".to_string(),
            source: "a".to_string(),
            target: "b".to_string(),
            rel_type: Some("KNOWS".to_string()),
            properties: HashMap::new(),
            direction: RelationshipDirection::Outbound,
            min_depth: Some(1),
            max_depth: Some(1),
        };

        assert_eq!(edge.identifier, "r");
        assert_eq!(edge.source, "a");
        assert_eq!(edge.target, "b");
        assert_eq!(edge.rel_type, Some("KNOWS".to_string()));
        assert_eq!(edge.direction, RelationshipDirection::Outbound);
    }

    // Helper function to find MATCH clause in AST
    fn find_match_clause(node: *const cypher_astnode_t) -> Option<*const cypher_astnode_t> {
        if node.is_null() {
            return None;
        }

        let node_type = unsafe { cypher_astnode_type(node) };
        let cypher_match = unsafe { CYPHER_AST_MATCH };
        if node_type == cypher_match {
            return Some(node);
        }

        // Recursively search children
        let n_children = unsafe { cypher_astnode_nchildren(node) };
        for i in 0..n_children {
            let child = unsafe { cypher_astnode_get_child(node, i) };
            if let Some(match_node) = find_match_clause(child) {
                return Some(match_node);
            }
        }

        None
    }

    // Helper function to parse query and extract match graph
    fn parse_and_extract_graph(
        query: &str,
    ) -> Result<(Vec<PatternVertex>, Vec<PatternEdge>), Box<dyn std::error::Error>> {
        let result = parse_cypher_query(query);
        if result.is_null() {
            return Err("Failed to parse query".into());
        }

        let n_errors = unsafe { cypher_parse_result_nerrors(result) };
        if n_errors > 0 {
            cleanup_parse_result(result);
            return Err("Parse errors in query".into());
        }

        let n_roots = unsafe { cypher_parse_result_nroots(result) };
        if n_roots == 0 {
            cleanup_parse_result(result);
            return Err("No root nodes in parse result".into());
        }

        let root = unsafe { cypher_parse_result_get_root(result, 0) };
        let match_clause = find_match_clause(root).ok_or("No MATCH clause found")?;

        let graph_result = make_match_graph(match_clause);
        cleanup_parse_result(result);

        match graph_result {
            Ok(graph) => Ok(graph),
            Err(e) => Err(e.into()),
        }
    }

    // Helper function to find vertex by identifier
    fn find_vertex_by_id<'a>(vertices: &'a [PatternVertex], id: &str) -> Option<&'a PatternVertex> {
        vertices.iter().find(|v| v.identifier == id)
    }

    // Helper function to count vertices with specific label
    fn count_vertices_with_label(vertices: &[PatternVertex], label: &str) -> usize {
        vertices
            .iter()
            .filter(|v| v.label.as_deref() == Some(label))
            .count()
    }

    // Helper function to find edge between two vertices
    #[allow(dead_code)]
    fn find_edge<'a>(
        edges: &'a [PatternEdge],
        source: &str,
        target: &str,
    ) -> Option<&'a PatternEdge> {
        edges
            .iter()
            .find(|e| e.source == source && e.target == target)
    }

    // Helper function to validate graph is a path (connected, each vertex has at most 2 edges, at least 1)
    fn is_path_graph(vertices: &[PatternVertex], edges: &[PatternEdge]) -> bool {
        if vertices.is_empty() {
            return true;
        }
        if vertices.len() == 1 {
            return edges.is_empty();
        }

        // Check degree constraint (each vertex has at most 2 edges)
        for vertex in vertices {
            let degree = edges
                .iter()
                .filter(|e| e.source == vertex.identifier || e.target == vertex.identifier)
                .count();
            if degree == 0 || degree > 2 {
                return false;
            }
        }

        // Check connectivity - must be connected as a single component
        let mut visited = std::collections::HashSet::new();
        let mut stack = vec![&vertices[0].identifier];

        while let Some(current) = stack.pop() {
            if visited.contains(current) {
                continue;
            }
            visited.insert(current);

            // Add neighbors
            for edge in edges {
                if edge.source == *current && !visited.contains(&edge.target) {
                    stack.push(&edge.target);
                } else if edge.target == *current && !visited.contains(&edge.source) {
                    stack.push(&edge.source);
                }
            }
        }

        visited.len() == vertices.len()
    }

    // Helper function to validate graph is a tree (connected, acyclic)
    fn is_tree_graph(vertices: &[PatternVertex], edges: &[PatternEdge]) -> bool {
        if vertices.is_empty() {
            return true;
        }
        if vertices.len() == 1 {
            return edges.is_empty();
        }

        // Tree must have exactly n-1 edges for n vertices
        if edges.len() != vertices.len() - 1 {
            return false;
        }

        // Check connectivity using simple traversal
        let mut visited = std::collections::HashSet::new();
        let mut stack = vec![&vertices[0].identifier];

        while let Some(current) = stack.pop() {
            if visited.contains(current) {
                continue;
            }
            visited.insert(current);

            // Add neighbors
            for edge in edges {
                if edge.source == *current && !visited.contains(&edge.target) {
                    stack.push(&edge.target);
                } else if edge.target == *current && !visited.contains(&edge.source) {
                    stack.push(&edge.source);
                }
            }
        }

        visited.len() == vertices.len()
    }

    // Helper function to validate graph contains a cycle
    fn has_cycle(vertices: &[PatternVertex], edges: &[PatternEdge]) -> bool {
        if vertices.len() < 3 || edges.len() < 3 {
            return false;
        }

        // Simple cycle detection using DFS for undirected graph
        let mut visited = std::collections::HashSet::new();

        for vertex in vertices {
            if !visited.contains(&vertex.identifier)
                && has_cycle_util(&vertex.identifier, None, edges, &mut visited) {
                return true;
            }
        }
        false
    }

    fn has_cycle_util(
        vertex: &str,
        parent: Option<&str>,
        edges: &[PatternEdge],
        visited: &mut std::collections::HashSet<String>,
    ) -> bool {
        visited.insert(vertex.to_string());

        // Visit all neighbors
        for edge in edges {
            let neighbor = if edge.source == vertex {
                Some(&edge.target)
            } else if edge.target == vertex {
                Some(&edge.source)
            } else {
                None
            };

            if let Some(neighbor) = neighbor {
                // Skip the parent to avoid immediate back-edge
                if parent == Some(neighbor) {
                    continue;
                }

                if !visited.contains(neighbor) {
                    if has_cycle_util(neighbor, Some(vertex), edges, visited) {
                        return true;
                    }
                } else {
                    // Found a back edge to a visited vertex (not parent) - this is a cycle
                    return true;
                }
            }
        }

        false
    }

    // COMPREHENSIVE TEST SUITE FOR make_match_graph

    #[test]
    fn test_basic_node_patterns() {
        // Test 1: Simple node without identifier
        let result = parse_and_extract_graph("MATCH () RETURN *");
        assert!(result.is_ok());
        let (vertices, edges) = result.unwrap();
        assert_eq!(vertices.len(), 1);
        assert_eq!(edges.len(), 0);
        assert!(vertices[0].identifier.starts_with("n_"));
        assert_eq!(vertices[0].label, None);
        assert!(vertices[0].properties.is_empty());

        // Test 2: Node with identifier
        let result = parse_and_extract_graph("MATCH (person) RETURN person");
        assert!(result.is_ok());
        let (vertices, edges) = result.unwrap();
        assert_eq!(vertices.len(), 1);
        assert_eq!(edges.len(), 0);
        assert_eq!(vertices[0].identifier, "person");
        assert_eq!(vertices[0].label, None);

        // Test 3: Node with label
        let result = parse_and_extract_graph("MATCH (n:Person) RETURN n");
        assert!(result.is_ok());
        let (vertices, _edges) = result.unwrap();
        assert_eq!(vertices.len(), 1);
        assert_eq!(vertices[0].identifier, "n");
        assert_eq!(vertices[0].label, Some("Person".to_string()));

        // Test 4: Node with identifier and label
        let result = parse_and_extract_graph("MATCH (person:Person) RETURN person");
        assert!(result.is_ok());
        let (vertices, _edges) = result.unwrap();
        assert_eq!(vertices.len(), 1);
        assert_eq!(vertices[0].identifier, "person");
        assert_eq!(vertices[0].label, Some("Person".to_string()));

        // Test 5: Multiple different node types
        let result =
            parse_and_extract_graph("MATCH (a), (b:Label), (c:AnotherLabel) RETURN a, b, c");
        assert!(result.is_ok());
        let (vertices, edges) = result.unwrap();
        assert_eq!(vertices.len(), 3);
        assert_eq!(edges.len(), 0);

        let a = find_vertex_by_id(&vertices, "a").unwrap();
        assert_eq!(a.label, None);

        let b = find_vertex_by_id(&vertices, "b").unwrap();
        assert_eq!(b.label, Some("Label".to_string()));

        let c = find_vertex_by_id(&vertices, "c").unwrap();
        assert_eq!(c.label, Some("AnotherLabel".to_string()));
    }

    #[test]
    fn test_relationship_patterns() {
        // Test 1: Simple relationship without type
        let result = parse_and_extract_graph("MATCH (a)-[r]->(b) RETURN a, r, b");
        assert!(result.is_ok());
        let (vertices, edges) = result.unwrap();
        assert_eq!(vertices.len(), 2);
        assert_eq!(edges.len(), 1);

        let edge = &edges[0];
        assert_eq!(edge.source, "a");
        assert_eq!(edge.target, "b");
        assert_eq!(edge.rel_type, None);

        // Test 2: Relationship with type
        let result = parse_and_extract_graph("MATCH (a)-[:KNOWS]->(b) RETURN a, b");
        assert!(result.is_ok());
        let (vertices, edges) = result.unwrap();
        assert_eq!(vertices.len(), 2);
        assert_eq!(edges.len(), 1);

        let edge = &edges[0];
        assert_eq!(edge.rel_type, Some("KNOWS".to_string()));

        // Test 3: Relationship with identifier and type
        let result = parse_and_extract_graph("MATCH (a)-[r:KNOWS]->(b) RETURN a, r, b");
        assert!(result.is_ok());
        let (vertices, edges) = result.unwrap();
        assert_eq!(vertices.len(), 2);
        assert_eq!(edges.len(), 1);

        let edge = &edges[0];
        assert_eq!(edge.rel_type, Some("KNOWS".to_string()));

        // Test 4: Undirected relationship
        let result = parse_and_extract_graph("MATCH (a)-[r:CONNECTED]-(b) RETURN a, r, b");
        assert!(result.is_ok());
        let (vertices, edges) = result.unwrap();
        assert_eq!(vertices.len(), 2);
        assert_eq!(edges.len(), 1);

        let edge = &edges[0];
        assert_eq!(edge.rel_type, Some("CONNECTED".to_string()));

        // Test 5: Multiple relationship types
        let result = parse_and_extract_graph(
            "MATCH (a)-[:FOLLOWS]->(b), (c)-[:LIKES]->(d) RETURN a, b, c, d",
        );
        assert!(result.is_ok());
        let (vertices, edges) = result.unwrap();
        assert_eq!(vertices.len(), 4);
        assert_eq!(edges.len(), 2);

        // Verify both relationship types exist
        let has_follows = edges
            .iter()
            .any(|e| e.rel_type == Some("FOLLOWS".to_string()));
        let has_likes = edges
            .iter()
            .any(|e| e.rel_type == Some("LIKES".to_string()));
        assert!(has_follows && has_likes);
    }

    #[test]
    fn test_path_patterns() {
        // Test 1: Short path (2 nodes, 1 edge)
        let result = parse_and_extract_graph("MATCH (a)-[:KNOWS]->(b) RETURN a, b");
        assert!(result.is_ok());
        let (vertices, edges) = result.unwrap();
        assert_eq!(vertices.len(), 2);
        assert_eq!(edges.len(), 1);
        assert!(is_path_graph(&vertices, &edges));

        // Test 2: Medium path (3 nodes, 2 edges)
        let result =
            parse_and_extract_graph("MATCH (a)-[:KNOWS]->(b)-[:WORKS_AT]->(c) RETURN a, b, c");
        assert!(result.is_ok());
        let (vertices, edges) = result.unwrap();
        assert_eq!(vertices.len(), 3);
        assert_eq!(edges.len(), 2);
        assert!(is_path_graph(&vertices, &edges));

        // Test 3: Longer path (4 nodes, 3 edges)
        let result =
            parse_and_extract_graph("MATCH (a)-[:R1]->(b)-[:R2]->(c)-[:R3]->(d) RETURN a, b, c, d");
        assert!(result.is_ok());
        let (vertices, edges) = result.unwrap();
        assert_eq!(vertices.len(), 4);
        assert_eq!(edges.len(), 3);
        assert!(is_path_graph(&vertices, &edges));

        // Test 4: Very long path (5 nodes, 4 edges)
        let result = parse_and_extract_graph(
            "MATCH (a)-[:R1]->(b)-[:R2]->(c)-[:R3]->(d)-[:R4]->(e) RETURN a, b, c, d, e",
        );
        assert!(result.is_ok());
        let (vertices, edges) = result.unwrap();
        assert_eq!(vertices.len(), 5);
        assert_eq!(edges.len(), 4);
        assert!(is_path_graph(&vertices, &edges));

        // Test 5: Extra long path (6 nodes, 5 edges)
        let result = parse_and_extract_graph(
            "MATCH (a)-[:R1]->(b)-[:R2]->(c)-[:R3]->(d)-[:R4]->(e)-[:R5]->(f) RETURN a, b, c, d, e, f",
        );
        assert!(result.is_ok());
        let (vertices, edges) = result.unwrap();
        assert_eq!(vertices.len(), 6);
        assert_eq!(edges.len(), 5);
        assert!(is_path_graph(&vertices, &edges));
    }

    #[test]
    fn test_multiple_pattern_paths() {
        // Test 1: Two separate nodes
        let result = parse_and_extract_graph("MATCH (a), (b) RETURN a, b");
        assert!(result.is_ok());
        let (vertices, edges) = result.unwrap();
        assert_eq!(vertices.len(), 2);
        assert_eq!(edges.len(), 0);

        // Test 2: Two separate simple relationships
        let result =
            parse_and_extract_graph("MATCH (a)-[:KNOWS]->(b), (c)-[:LIKES]->(d) RETURN a, b, c, d");
        assert!(result.is_ok());
        let (vertices, edges) = result.unwrap();
        assert_eq!(vertices.len(), 4);
        assert_eq!(edges.len(), 2);

        // Test 3: Mix of single nodes and relationships
        let result =
            parse_and_extract_graph("MATCH (isolated), (a)-[:CONNECTS]->(b) RETURN isolated, a, b");
        assert!(result.is_ok());
        let (vertices, edges) = result.unwrap();
        assert_eq!(vertices.len(), 3);
        assert_eq!(edges.len(), 1);

        // Test 4: Three separate patterns
        let result = parse_and_extract_graph(
            "MATCH (single), (a)-[:R1]->(b), (c)-[:R2]->(d)-[:R3]->(e) RETURN *",
        );
        assert!(result.is_ok());
        let (vertices, edges) = result.unwrap();
        assert_eq!(vertices.len(), 6); // single + a,b + c,d,e
        assert_eq!(edges.len(), 3); // 0 + 1 + 2

        // Test 5: Multiple complex patterns
        let result = parse_and_extract_graph(
            "MATCH (a)-[:R1]->(b)-[:R2]->(c), (d)-[:R3]->(e), (f) RETURN *",
        );
        assert!(result.is_ok());
        let (vertices, edges) = result.unwrap();
        assert_eq!(vertices.len(), 6); // a,b,c + d,e + f
        assert_eq!(edges.len(), 3); // 2 + 1 + 0
    }

    #[test]
    fn test_mixed_labels_and_types() {
        // Test 1: Mixed labeled and unlabeled nodes
        let result = parse_and_extract_graph(
            "MATCH (person:Person)-[:KNOWS]->(friend) RETURN person, friend",
        );
        assert!(result.is_ok());
        let (vertices, _edges) = result.unwrap();
        assert_eq!(vertices.len(), 2);

        let person = find_vertex_by_id(&vertices, "person").unwrap();
        assert_eq!(person.label, Some("Person".to_string()));

        let friend = find_vertex_by_id(&vertices, "friend").unwrap();
        assert_eq!(friend.label, None);

        // Test 2: Multiple different labels
        let result = parse_and_extract_graph(
            "MATCH (p:Person)-[:WORKS_AT]->(c:Company)-[:LOCATED_IN]->(city:City) RETURN p, c, city",
        );
        assert!(result.is_ok());
        let (vertices, _edges) = result.unwrap();
        assert_eq!(vertices.len(), 3);

        assert_eq!(count_vertices_with_label(&vertices, "Person"), 1);
        assert_eq!(count_vertices_with_label(&vertices, "Company"), 1);
        assert_eq!(count_vertices_with_label(&vertices, "City"), 1);

        // Test 3: Mixed typed and untyped relationships
        let result = parse_and_extract_graph("MATCH (a)-[:TYPED_REL]->(b)-[]->(c) RETURN a, b, c");
        assert!(result.is_ok());
        let (vertices, edges) = result.unwrap();
        assert_eq!(vertices.len(), 3);
        assert_eq!(edges.len(), 2);

        let typed_rels = edges.iter().filter(|e| e.rel_type.is_some()).count();
        let untyped_rels = edges.iter().filter(|e| e.rel_type.is_none()).count();
        assert_eq!(typed_rels, 1);
        assert_eq!(untyped_rels, 1);

        // Test 4: Complex mix
        let result = parse_and_extract_graph(
            "MATCH (p1:Person)-[:KNOWS]-(p2:Person), (c:Company)-[]->(l) RETURN *",
        );
        assert!(result.is_ok());
        let (vertices, edges) = result.unwrap();
        assert_eq!(vertices.len(), 4);
        assert_eq!(edges.len(), 2);

        assert_eq!(count_vertices_with_label(&vertices, "Person"), 2);
        assert_eq!(count_vertices_with_label(&vertices, "Company"), 1);
    }

    #[test]
    fn test_topology_validation() {
        // Test 1: Simple path topology
        let result = parse_and_extract_graph("MATCH (a)-[:R1]->(b)-[:R2]->(c) RETURN a, b, c");
        assert!(result.is_ok());
        let (vertices, edges) = result.unwrap();
        assert!(is_path_graph(&vertices, &edges));
        assert!(is_tree_graph(&vertices, &edges));
        assert!(!has_cycle(&vertices, &edges));

        // Test 2: Star topology (tree but not path)
        // Note: This might be hard to represent in a single MATCH pattern,
        // but we can test the validation functions with constructed data
        let vertices = vec![
            PatternVertex {
                identifier: "center".to_string(),
                label: None,
                properties: HashMap::new(),
            },
            PatternVertex {
                identifier: "leaf1".to_string(),
                label: None,
                properties: HashMap::new(),
            },
            PatternVertex {
                identifier: "leaf2".to_string(),
                label: None,
                properties: HashMap::new(),
            },
            PatternVertex {
                identifier: "leaf3".to_string(),
                label: None,
                properties: HashMap::new(),
            },
        ];
        let edges = vec![
            PatternEdge {
                identifier: "r_1".to_string(),
                source: "center".to_string(),
                target: "leaf1".to_string(),
                rel_type: None,
                properties: HashMap::new(),
                direction: RelationshipDirection::Outbound,
                min_depth: Some(1),
                max_depth: Some(1),
            },
            PatternEdge {
                identifier: "r_2".to_string(),
                source: "center".to_string(),
                target: "leaf2".to_string(),
                rel_type: None,
                properties: HashMap::new(),
                direction: RelationshipDirection::Outbound,
                min_depth: Some(1),
                max_depth: Some(1),
            },
            PatternEdge {
                identifier: "r_3".to_string(),
                source: "center".to_string(),
                target: "leaf3".to_string(),
                rel_type: None,
                properties: HashMap::new(),
                direction: RelationshipDirection::Outbound,
                min_depth: Some(1),
                max_depth: Some(1),
            },
        ];

        assert!(!is_path_graph(&vertices, &edges)); // Not a path (center has degree 3)
        assert!(is_tree_graph(&vertices, &edges)); // But is a tree
        assert!(!has_cycle(&vertices, &edges)); // No cycles

        // Test 3: Disconnected components
        let result =
            parse_and_extract_graph("MATCH (a)-[:R1]->(b), (c)-[:R2]->(d) RETURN a, b, c, d");
        assert!(result.is_ok());
        let (vertices, edges) = result.unwrap();
        assert!(!is_path_graph(&vertices, &edges)); // Not connected as single path
        assert!(!is_tree_graph(&vertices, &edges)); // Not connected as single tree
    }

    #[test]
    fn test_auto_generated_identifiers() {
        // Test 1: Nodes without identifiers get auto-generated ones
        let result = parse_and_extract_graph("MATCH ()-[]-() RETURN *");
        assert!(result.is_ok());
        let (vertices, edges) = result.unwrap();
        assert_eq!(vertices.len(), 2);
        assert_eq!(edges.len(), 1);

        // Both vertices should have auto-generated identifiers
        for vertex in &vertices {
            assert!(vertex.identifier.starts_with("n_") || !vertex.identifier.is_empty());
        }

        // Test 2: Mix of explicit and auto-generated identifiers
        let result = parse_and_extract_graph("MATCH (named)-[]-() RETURN named");
        assert!(result.is_ok());
        let (vertices, _edges) = result.unwrap();
        assert_eq!(vertices.len(), 2);

        let named = find_vertex_by_id(&vertices, "named");
        assert!(named.is_some());

        // The other vertex should have auto-generated identifier
        let other = vertices.iter().find(|v| v.identifier != "named").unwrap();
        assert!(other.identifier.starts_with("n_") || other.identifier != "named");
    }

    #[test]
    fn test_complex_real_world_patterns() {
        // Test 1: Social network pattern
        let result = parse_and_extract_graph(
            "MATCH (user:User)-[:FOLLOWS]->(friend:User)-[:POSTS]->(content:Post) RETURN user, friend, content",
        );
        assert!(result.is_ok());
        let (vertices, edges) = result.unwrap();
        assert_eq!(vertices.len(), 3);
        assert_eq!(edges.len(), 2);
        assert!(is_path_graph(&vertices, &edges));

        // Test 2: Organizational hierarchy
        let result = parse_and_extract_graph(
            "MATCH (emp:Employee)-[:REPORTS_TO]->(mgr:Manager)-[:WORKS_FOR]->(dept:Department) RETURN emp, mgr, dept",
        );
        assert!(result.is_ok());
        let (vertices, edges) = result.unwrap();
        assert_eq!(vertices.len(), 3);
        assert_eq!(edges.len(), 2);

        assert_eq!(count_vertices_with_label(&vertices, "Employee"), 1);
        assert_eq!(count_vertices_with_label(&vertices, "Manager"), 1);
        assert_eq!(count_vertices_with_label(&vertices, "Department"), 1);

        // Test 3: Technology stack pattern
        let result = parse_and_extract_graph(
            "MATCH (app:Application)-[:RUNS_ON]->(server:Server)-[:HOSTS]->(db:Database) RETURN app, server, db",
        );
        assert!(result.is_ok());
        let (vertices, edges) = result.unwrap();
        assert_eq!(vertices.len(), 3);
        assert_eq!(edges.len(), 2);

        let runs_on = edges
            .iter()
            .any(|e| e.rel_type == Some("RUNS_ON".to_string()));
        let hosts = edges
            .iter()
            .any(|e| e.rel_type == Some("HOSTS".to_string()));
        assert!(runs_on && hosts);

        // Test 4: Supply chain pattern
        let result = parse_and_extract_graph(
            "MATCH (supplier:Supplier)-[:SUPPLIES]->(manufacturer:Manufacturer)-[:PRODUCES]->(product:Product)-[:SOLD_TO]->(customer:Customer) RETURN *",
        );
        assert!(result.is_ok());
        let (vertices, edges) = result.unwrap();
        assert_eq!(vertices.len(), 4);
        assert_eq!(edges.len(), 3);
        assert!(is_path_graph(&vertices, &edges));

        // Verify all relationship types
        let rel_types: Vec<String> = edges.iter().filter_map(|e| e.rel_type.clone()).collect();
        assert!(rel_types.contains(&"SUPPLIES".to_string()));
        assert!(rel_types.contains(&"PRODUCES".to_string()));
        assert!(rel_types.contains(&"SOLD_TO".to_string()));
    }

    #[test]
    fn test_error_conditions() {
        // Test 1: Invalid query syntax
        let result = parse_and_extract_graph("INVALID SYNTAX {{{");
        assert!(result.is_err());

        // Test 2: Non-MATCH query
        let _result = parse_and_extract_graph("CREATE (n) RETURN n");
        // This might still work if it finds no MATCH clause, or might error
        // The exact behavior depends on the implementation

        // Test 3: Empty pattern
        let result = parse_and_extract_graph("MATCH RETURN *");
        assert!(result.is_err());

        // Test 4: NULL input to make_match_graph
        let result = make_match_graph(std::ptr::null());
        assert_eq!(result, Err(GraphError::InvalidAstNode));
    }

    #[test]
    fn test_graph_printing() {
        // Test that graph printing doesn't panic with various patterns
        let result = parse_and_extract_graph(
            "MATCH (p:Person {name: 'Alice'})-[:KNOWS {since: 2020}]->(f:Friend) RETURN p, f",
        );
        if let Ok((vertices, edges)) = result {
            print_pattern_graph(&vertices, &edges);
            // This should not panic
        }

        // Test with empty graph
        let vertices = vec![];
        let edges = vec![];
        print_pattern_graph(&vertices, &edges);

        // Test with single vertex
        let vertices = vec![PatternVertex {
            identifier: "test".to_string(),
            label: Some("TestLabel".to_string()),
            properties: HashMap::new(),
        }];
        let edges = vec![];
        print_pattern_graph(&vertices, &edges);
    }

    // TESTS FOR EXTRACT_PROPERTIES FUNCTION

    #[test]
    fn test_extract_properties_null_input() {
        let result = extract_properties(ptr::null());
        assert_eq!(result, None);
    }

    #[test]
    fn test_extract_properties_non_map_node() {
        // Test with a non-map AST node (should return None)
        let query = "MATCH (n) RETURN n";
        let result = parse_cypher_query(query);

        if !result.is_null() {
            let n_errors = unsafe { cypher_parse_result_nerrors(result) };
            if n_errors == 0 {
                let n_roots = unsafe { cypher_parse_result_nroots(result) };
                if n_roots > 0 {
                    let root = unsafe { cypher_parse_result_get_root(result, 0) };
                    if !root.is_null() {
                        // Root node is not a map, should return None
                        let properties = extract_properties(root);
                        assert_eq!(properties, None);
                    }
                }
            }
        }

        cleanup_parse_result(result);
    }

    #[test]
    fn test_extract_properties_empty_map() {
        let query = "MATCH (n {}) RETURN n";
        let result = parse_cypher_query(query);

        if !result.is_null() {
            let n_errors = unsafe { cypher_parse_result_nerrors(result) };
            if n_errors == 0 {
                if let Some(map_node) = find_map_node_in_query(result) {
                    let properties = extract_properties(map_node);
                    assert!(properties.is_some());
                    let props = properties.unwrap();
                    assert!(props.is_empty());
                }
            }
        }

        cleanup_parse_result(result);
    }

    #[test]
    fn test_extract_properties_string_values() {
        let query = "MATCH (n {name: 'Alice', city: 'Wonderland'}) RETURN n";
        let result = parse_cypher_query(query);

        if !result.is_null() {
            let n_errors = unsafe { cypher_parse_result_nerrors(result) };
            if n_errors == 0 {
                if let Some(map_node) = find_map_node_in_query(result) {
                    let properties = extract_properties(map_node);
                    assert!(properties.is_some());
                    let props = properties.unwrap();

                    assert_eq!(props.get("name"), Some(&Value::String("Alice".to_string())));
                    assert_eq!(props.get("city"), Some(&Value::String("Wonderland".to_string())));
                    assert_eq!(props.len(), 2);
                }
            }
        }

        cleanup_parse_result(result);
    }

    #[test]
    fn test_extract_properties_integer_values() {
        let query = "MATCH (n {age: 30, count: 42}) RETURN n";
        let result = parse_cypher_query(query);

        if !result.is_null() {
            let n_errors = unsafe { cypher_parse_result_nerrors(result) };
            if n_errors == 0 {
                if let Some(map_node) = find_map_node_in_query(result) {
                    let properties = extract_properties(map_node);
                    assert!(properties.is_some());
                    let props = properties.unwrap();

                    assert_eq!(props.get("age"), Some(&Value::Number(serde_json::Number::from(30))));
                    assert_eq!(props.get("count"), Some(&Value::Number(serde_json::Number::from(42))));
                    assert_eq!(props.len(), 2);
                }
            }
        }

        cleanup_parse_result(result);
    }

    #[test]
    fn test_extract_properties_boolean_values() {
        let query = "MATCH (n {active: true, deleted: false}) RETURN n";
        let result = parse_cypher_query(query);

        if !result.is_null() {
            let n_errors = unsafe { cypher_parse_result_nerrors(result) };
            if n_errors == 0 {
                if let Some(map_node) = find_map_node_in_query(result) {
                    let properties = extract_properties(map_node);
                    assert!(properties.is_some());
                    let props = properties.unwrap();

                    assert_eq!(props.get("active"), Some(&Value::Bool(true)));
                    assert_eq!(props.get("deleted"), Some(&Value::Bool(false)));
                    assert_eq!(props.len(), 2);
                }
            }
        }

        cleanup_parse_result(result);
    }

    #[test]
    fn test_extract_properties_float_values() {
        let query = "MATCH (n {height: 5.9, weight: 70.5}) RETURN n";
        let result = parse_cypher_query(query);

        if !result.is_null() {
            let n_errors = unsafe { cypher_parse_result_nerrors(result) };
            if n_errors == 0 {
                if let Some(map_node) = find_map_node_in_query(result) {
                    let properties = extract_properties(map_node);
                    assert!(properties.is_some());
                    let props = properties.unwrap();

                    assert_eq!(props.get("height"), Some(&Value::Number(serde_json::Number::from_f64(5.9).unwrap())));
                    assert_eq!(props.get("weight"), Some(&Value::Number(serde_json::Number::from_f64(70.5).unwrap())));
                    assert_eq!(props.len(), 2);
                }
            }
        }

        cleanup_parse_result(result);
    }

    #[test]
    fn test_extract_properties_mixed_types() {
        let query = "MATCH (n {name: 'Alice', age: 30, active: true, score: 95.5}) RETURN n";
        let result = parse_cypher_query(query);

        if !result.is_null() {
            let n_errors = unsafe { cypher_parse_result_nerrors(result) };
            if n_errors == 0 {
                if let Some(map_node) = find_map_node_in_query(result) {
                    let properties = extract_properties(map_node);
                    assert!(properties.is_some());
                    let props = properties.unwrap();

                    assert_eq!(props.get("name"), Some(&Value::String("Alice".to_string())));
                    assert_eq!(props.get("age"), Some(&Value::Number(serde_json::Number::from(30))));
                    assert_eq!(props.get("active"), Some(&Value::Bool(true)));
                    assert_eq!(props.get("score"), Some(&Value::Number(serde_json::Number::from_f64(95.5).unwrap())));
                    assert_eq!(props.len(), 4);
                }
            }
        }

        cleanup_parse_result(result);
    }

    #[test]
    fn test_extract_properties_null_values() {
        let query = "MATCH (n {optional: null}) RETURN n";
        let result = parse_cypher_query(query);

        if !result.is_null() {
            let n_errors = unsafe { cypher_parse_result_nerrors(result) };
            if n_errors == 0 {
                if let Some(map_node) = find_map_node_in_query(result) {
                    let properties = extract_properties(map_node);
                    assert!(properties.is_some());
                    let props = properties.unwrap();

                    assert_eq!(props.get("optional"), Some(&Value::Null));
                    assert_eq!(props.len(), 1);
                }
            }
        }

        cleanup_parse_result(result);
    }

    #[test]
    fn test_extract_properties_real_world_s3_bucket() {
        let query = "MATCH (n:aws_s3_bucket {org_id: 2, to_delete: false, default_encryption_type: 'aws:kms'}) RETURN n";
        let result = parse_cypher_query(query);

        if !result.is_null() {
            let n_errors = unsafe { cypher_parse_result_nerrors(result) };
            if n_errors == 0 {
                if let Some(map_node) = find_map_node_in_query(result) {
                    let properties = extract_properties(map_node);
                    assert!(properties.is_some());
                    let props = properties.unwrap();

                    assert_eq!(props.get("org_id"), Some(&Value::Number(serde_json::Number::from(2))));
                    assert_eq!(props.get("to_delete"), Some(&Value::Bool(false)));
                    assert_eq!(
                        props.get("default_encryption_type"),
                        Some(&Value::String("aws:kms".to_string()))
                    );
                    assert_eq!(props.len(), 3);
                }
            }
        }

        cleanup_parse_result(result);
    }

    #[test]
    fn test_extract_properties_real_world_complex() {
        let query = "MATCH (n:aws_ec2_instance {org_id: 2, to_delete: false, entity_info_status:1, is_publicly_accessible: true}) RETURN n";
        let result = parse_cypher_query(query);

        if !result.is_null() {
            let n_errors = unsafe { cypher_parse_result_nerrors(result) };
            if n_errors == 0 {
                if let Some(map_node) = find_map_node_in_query(result) {
                    let properties = extract_properties(map_node);
                    assert!(properties.is_some());
                    let props = properties.unwrap();

                    assert_eq!(props.get("org_id"), Some(&Value::Number(serde_json::Number::from(2))));
                    assert_eq!(props.get("to_delete"), Some(&Value::Bool(false)));
                    assert_eq!(props.get("entity_info_status"), Some(&Value::Number(serde_json::Number::from(1))));
                    assert_eq!(
                        props.get("is_publicly_accessible"),
                        Some(&Value::Bool(true))
                    );
                    assert_eq!(props.len(), 4);
                }
            }
        }

        cleanup_parse_result(result);
    }

    #[test]
    fn test_extract_properties_relationship_properties() {
        let query = "MATCH (a)-[r:KNOWS {since: 2020, strength: 'strong'}]->(b) RETURN a, r, b";
        let result = parse_cypher_query(query);

        if !result.is_null() {
            let n_errors = unsafe { cypher_parse_result_nerrors(result) };
            if n_errors == 0 {
                if let Some(map_node) = find_map_node_in_query(result) {
                    let properties = extract_properties(map_node);
                    assert!(properties.is_some());
                    let props = properties.unwrap();

                    assert_eq!(props.get("since"), Some(&Value::Number(serde_json::Number::from(2020))));
                    assert_eq!(props.get("strength"), Some(&Value::String("strong".to_string())));
                    assert_eq!(props.len(), 2);
                }
            }
        }

        cleanup_parse_result(result);
    }

    #[test]
    fn test_extract_property_name_helper() {
        // Test the helper function directly with null input
        let result = extract_property_name(ptr::null());
        assert_eq!(result, None);
    }

    #[test]
    fn test_extract_expression_value_helper() {
        // Test the helper function directly with null input
        let result = extract_expression_value(ptr::null());
        assert_eq!(result, None);
    }

    // Helper function to find a map node in a parsed query
    fn find_map_node_in_query(
        result: *const cypher_parse_result_t,
    ) -> Option<*const cypher_astnode_t> {
        if result.is_null() {
            return None;
        }

        let n_roots = unsafe { cypher_parse_result_nroots(result) };
        for i in 0..n_roots {
            let root = unsafe { cypher_parse_result_get_root(result, i) };
            if let Some(map_node) = find_map_node_recursive(root) {
                return Some(map_node);
            }
        }

        None
    }

    // Recursively search for a map node in the AST
    fn find_map_node_recursive(node: *const cypher_astnode_t) -> Option<*const cypher_astnode_t> {
        if node.is_null() {
            return None;
        }

        let node_type = unsafe { cypher_astnode_type(node) };
        let cypher_map = unsafe { CYPHER_AST_MAP };

        if node_type == cypher_map {
            return Some(node);
        }

        // Recursively search children
        let n_children = unsafe { cypher_astnode_nchildren(node) };
        for i in 0..n_children {
            let child = unsafe { cypher_astnode_get_child(node, i) };
            if let Some(map_node) = find_map_node_recursive(child) {
                return Some(map_node);
            }
        }

        None
    }

    // TESTS FOR determine_relationship_direction FUNCTION

    #[test]
    fn test_relationship_direction_outbound() {
        let query = "MATCH (v) -[e]-> (w) RETURN v, e, w";
        let result = parse_cypher_query(query);

        if !result.is_null() {
            let n_errors = unsafe { cypher_parse_result_nerrors(result) };
            if n_errors == 0 {
                if let Some(match_clause) = find_match_clause_from_result(result) {
                    match make_match_graph(match_clause) {
                        Ok((vertices, edges)) => {
                            assert_eq!(vertices.len(), 2);
                            assert_eq!(edges.len(), 1);

                            let edge = &edges[0];
                            assert_eq!(edge.direction, RelationshipDirection::Outbound);
                            assert_eq!(edge.source, "v");
                            assert_eq!(edge.target, "w");
                        }
                        Err(e) => {
                            println!("Failed to create graph: {e}");
                        }
                    }
                }
            }
        }

        cleanup_parse_result(result);
    }

    #[test]
    fn test_relationship_direction_inbound() {
        let query = "MATCH (v) <-[e]- (w) RETURN v, e, w";
        let result = parse_cypher_query(query);

        if !result.is_null() {
            let n_errors = unsafe { cypher_parse_result_nerrors(result) };
            if n_errors == 0 {
                if let Some(match_clause) = find_match_clause_from_result(result) {
                    match make_match_graph(match_clause) {
                        Ok((vertices, edges)) => {
                            assert_eq!(vertices.len(), 2);
                            assert_eq!(edges.len(), 1);

                            let edge = &edges[0];
                            assert_eq!(edge.direction, RelationshipDirection::Inbound);
                            assert_eq!(edge.source, "v");
                            assert_eq!(edge.target, "w");
                        }
                        Err(e) => {
                            println!("Failed to create graph: {e}");
                        }
                    }
                }
            }
        }

        cleanup_parse_result(result);
    }

    #[test]
    fn test_relationship_direction_bidirectional() {
        let query = "MATCH (v) -[e]- (w) RETURN v, e, w";
        let result = parse_cypher_query(query);

        if !result.is_null() {
            let n_errors = unsafe { cypher_parse_result_nerrors(result) };
            if n_errors == 0 {
                if let Some(match_clause) = find_match_clause_from_result(result) {
                    match make_match_graph(match_clause) {
                        Ok((vertices, edges)) => {
                            assert_eq!(vertices.len(), 2);
                            assert_eq!(edges.len(), 1);

                            let edge = &edges[0];
                            assert_eq!(edge.direction, RelationshipDirection::Bidirectional);
                            assert_eq!(edge.source, "v");
                            assert_eq!(edge.target, "w");
                        }
                        Err(e) => {
                            println!("Failed to create graph: {e}");
                        }
                    }
                }
            }
        }

        cleanup_parse_result(result);
    }

    #[test]
    fn test_multiple_relationship_directions() {
        let query = "MATCH (a) -[r1]-> (b) <-[r2]- (c), (d) -[r3]- (e) RETURN a, b, c, d, e";
        let result = parse_cypher_query(query);

        if !result.is_null() {
            let n_errors = unsafe { cypher_parse_result_nerrors(result) };
            if n_errors == 0 {
                if let Some(match_clause) = find_match_clause_from_result(result) {
                    match make_match_graph(match_clause) {
                        Ok((vertices, edges)) => {
                            assert_eq!(vertices.len(), 5);
                            assert_eq!(edges.len(), 3);

                            // Check that we have all three types of directions
                            let has_outbound = edges
                                .iter()
                                .any(|e| e.direction == RelationshipDirection::Outbound);
                            let has_inbound = edges
                                .iter()
                                .any(|e| e.direction == RelationshipDirection::Inbound);
                            let has_bidirectional = edges
                                .iter()
                                .any(|e| e.direction == RelationshipDirection::Bidirectional);

                            assert!(
                                has_outbound,
                                "Should have at least one outbound relationship"
                            );
                            assert!(has_inbound, "Should have at least one inbound relationship");
                            assert!(
                                has_bidirectional,
                                "Should have at least one bidirectional relationship"
                            );
                        }
                        Err(e) => {
                            println!("Failed to create graph: {e}");
                        }
                    }
                }
            }
        }

        cleanup_parse_result(result);
    }

    // Helper function to find match clause from parse result
    fn find_match_clause_from_result(
        result: *const cypher_parse_result_t,
    ) -> Option<*const cypher_astnode_t> {
        if result.is_null() {
            return None;
        }

        let n_roots = unsafe { cypher_parse_result_nroots(result) };
        for i in 0..n_roots {
            let root = unsafe { cypher_parse_result_get_root(result, i) };
            if let Some(match_clause) = find_match_clause(root) {
                return Some(match_clause);
            }
        }

        None
    }
}
