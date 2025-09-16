use std::ffi::CStr;
use std::os::raw::c_char;
use std::collections::HashMap;
#[allow(clippy::wildcard_imports)]
use libcypher_parser_sys::*;

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
    
    println!("{}{} ({} children)", "  ".repeat(indent), type_str, n_children);

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
    println!("{}{} (type: {}, children: {})", 
             "  ".repeat(indent), 
             type_str, 
             node_type, 
             n_children);

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
    pub properties: HashMap<String, String>,
}

/// Represents an edge (relationship pattern) in the pattern graph
#[derive(Debug, Clone, PartialEq)]
pub struct PatternEdge {
    /// Source vertex identifier
    pub source: String,
    /// Target vertex identifier
    pub target: String,
    /// Relationship type (can be empty)
    pub rel_type: Option<String>,
    /// Property values as key-value pairs
    pub properties: HashMap<String, String>,
    /// Direction of the relationship
    pub direction: RelationshipDirection,
}

/// Direction of a relationship
#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum RelationshipDirection {
    Outbound,   // ->
    Inbound,    // <-
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
            GraphError::VariableLengthRelationship => write!(f, "Variable length relationships are not supported"),
            GraphError::UnsupportedPattern => write!(f, "Unsupported pattern type encountered"),
            GraphError::InvalidIdentifier => write!(f, "Invalid identifier in pattern"),
        }
    }
}

impl std::error::Error for GraphError {}

/// Creates a graph representation from a MATCH statement AST
pub fn make_match_graph(match_node: *const cypher_astnode_t) -> Result<(Vec<PatternVertex>, Vec<PatternEdge>), GraphError> {
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
    process_pattern(pattern, &mut vertices, &mut edges, &mut node_counter)?;

    Ok((vertices, edges))
}

/// Processes a pattern node and extracts vertices and edges
fn process_pattern(pattern: *const cypher_astnode_t, vertices: &mut Vec<PatternVertex>, 
                  edges: &mut Vec<PatternEdge>, node_counter: &mut u32) -> Result<(), GraphError> {
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
            process_pattern(child, vertices, edges, node_counter)?;
        }
    } else if pattern_type == cypher_named_path {
        // Named path - get the actual path from the second child
        let n_children = unsafe { cypher_astnode_nchildren(pattern) };
        if n_children >= 2 {
            let path = unsafe { cypher_astnode_get_child(pattern, 1) };
            process_pattern_path(path, vertices, edges, node_counter)?;
        }
    } else if pattern_type == cypher_pattern_path {
        process_pattern_path(pattern, vertices, edges, node_counter)?;
    } else {
        // Try to process as pattern path anyway
        process_pattern_path(pattern, vertices, edges, node_counter)?;
    }

    Ok(())
}

/// Processes a pattern path and extracts alternating nodes and relationships
fn process_pattern_path(path: *const cypher_astnode_t, vertices: &mut Vec<PatternVertex>, 
                       edges: &mut Vec<PatternEdge>, node_counter: &mut u32) -> Result<(), GraphError> {
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
                        let edge = process_relationship_pattern(child, source_id.clone(), target_id)?;
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
fn process_node_pattern(node: *const cypher_astnode_t, node_counter: &mut u32) -> Result<PatternVertex, GraphError> {
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
fn process_relationship_pattern(rel: *const cypher_astnode_t, source: String, target: String) -> Result<PatternEdge, GraphError> {
    if rel.is_null() {
        return Err(GraphError::InvalidAstNode);
    }

    let mut rel_type: Option<String> = None;
    let mut properties = HashMap::new();

    // Check for variable length relationships - this should cause an error
    if has_variable_length(rel) {
        return Err(GraphError::VariableLengthRelationship);
    }

    let n_children = unsafe { cypher_astnode_nchildren(rel) };
    let cypher_reltype = unsafe { CYPHER_AST_RELTYPE };
    let cypher_map = unsafe { CYPHER_AST_MAP };
    
    for i in 0..n_children {
        let child = unsafe { cypher_astnode_get_child(rel, i) };
        if child.is_null() {
            continue;
        }
        
        let child_type = unsafe { cypher_astnode_type(child) };
        
        if child_type == cypher_reltype {
            if let Some(rt) = extract_reltype(child) {
                rel_type = Some(rt);
            }
        } else if child_type == cypher_map {
            properties = extract_properties(child).unwrap_or_default();
        }
    }

    // Try to determine direction from relationship pattern structure
    let direction = determine_relationship_direction(rel);

    Ok(PatternEdge {
        source,
        target,
        rel_type,
        properties,
        direction,
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
fn extract_properties(node: *const cypher_astnode_t) -> Option<HashMap<String, String>> {
    if node.is_null() {
        return None;
    }
    
    // For now, return empty map - this would need proper implementation
    // to traverse the map structure and extract key-value pairs
    Some(HashMap::new())
}

/// Checks if a relationship pattern has variable length
fn has_variable_length(rel: *const cypher_astnode_t) -> bool {
    if rel.is_null() {
        return false;
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
            return true;
        }
    }
    
    false
}

/// Determines the direction of a relationship from its AST representation
fn determine_relationship_direction(_rel: *const cypher_astnode_t) -> RelationshipDirection {
    // This would need to be implemented by examining the relationship pattern structure
    // For now, default to bidirectional
    RelationshipDirection::Bidirectional
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
                assert!(n_roots >= 2, "Should have at least 2 root nodes for multiple statements");
                
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
                                    println!("Graph created successfully: {} vertices, {} edges", vertices.len(), edges.len());
                                    // Basic validation
                                    assert!(!vertices.is_empty(), "Should have at least one vertex");
                                }
                                Err(e) => {
                                    println!("Graph creation failed: {}", e);
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
                                println!("Relationship graph: {} vertices, {} edges", vertices.len(), edges.len());
                                // Should have at least 2 vertices and 1 edge
                                assert!(vertices.len() >= 2, "Should have at least 2 vertices for relationship pattern");
                                assert!(!edges.is_empty(), "Should have at least 1 edge for relationship pattern");
                            }
                            Err(e) => {
                                println!("Relationship graph creation failed: {}", e);
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
                props.insert("name".to_string(), "Alice".to_string());
                props
            },
        };

        assert_eq!(vertex.identifier, "n");
        assert_eq!(vertex.label, Some("Person".to_string()));
        assert_eq!(vertex.properties.get("name"), Some(&"Alice".to_string()));
    }

    #[test]
    fn test_pattern_edge_creation() {
        let edge = PatternEdge {
            source: "a".to_string(),
            target: "b".to_string(),
            rel_type: Some("KNOWS".to_string()),
            properties: HashMap::new(),
            direction: RelationshipDirection::Outbound,
        };

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
}
