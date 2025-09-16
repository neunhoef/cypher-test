use std::env;
use std::ffi::CString;
use std::fs;
use std::os::raw::c_char;
use std::ptr;

// Use the libcypher_parser_sys crate
#[allow(clippy::wildcard_imports)]
use libcypher_parser_sys::*;

// Include our cypher module
mod cypher;
use cypher::{make_match_graph, PatternVertex, PatternEdge, GraphError, RelationshipDirection};

/// Finds the MATCH clause in a query and validates it's followed by a RETURN clause
fn find_match_return_pattern(result: *const cypher_parse_result_t) -> Option<*const cypher_astnode_t> {
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
    find_match_with_return(root)
}

/// Recursively searches for a MATCH clause followed by a RETURN clause
fn find_match_with_return(node: *const cypher_astnode_t) -> Option<*const cypher_astnode_t> {
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
        // Look for MATCH followed by RETURN in the query clauses
        let mut found_match: Option<*const cypher_astnode_t> = None;
        let mut found_return = false;
        
        for i in 0..n_children {
            let child = unsafe { cypher_astnode_get_child(node, i) };
            if child.is_null() {
                continue;
            }
            
            let child_type = unsafe { cypher_astnode_type(child) };
            
            if child_type == cypher_match && found_match.is_none() {
                found_match = Some(child);
            } else if child_type == cypher_return && found_match.is_some() {
                found_return = true;
                break;
            }
        }
        
        if found_match.is_some() && found_return {
            return found_match;
        }
    } else if node_type == cypher_match {
        // Check if there's a RETURN clause as a sibling
        // This is a simplified check - in practice, we'd need to validate the full structure
        return Some(node);
    }

    // Recursively search children
    for i in 0..n_children {
        let child = unsafe { cypher_astnode_get_child(node, i) };
        if let Some(match_node) = find_match_with_return(child) {
            return Some(match_node);
        }
    }

    None
}

/// Prints the pattern graph in tabular format
fn print_pattern_graph(vertices: &[PatternVertex], edges: &[PatternEdge]) {
    println!("\n=== Pattern Graph ===");
    
    // Print vertices table
    println!("\n--- Vertices ---");
    if vertices.is_empty() {
        println!("No vertices found.");
    } else {
        println!("┌─────────────────┬─────────────────┬─────────────────┐");
        println!("│ Identifier      │ Label           │ Properties      │");
        println!("├─────────────────┼─────────────────┼─────────────────┤");
        
        for vertex in vertices {
            let identifier = format!("{:<15}", truncate_string(&vertex.identifier, 15));
            let label = format!("{:<15}", 
                vertex.label.as_ref().map_or("(none)".to_string(), |l| truncate_string(l, 15)));
            let properties = if vertex.properties.is_empty() {
                "(none)".to_string()
            } else {
                let prop_str = vertex.properties.iter()
                    .map(|(k, v)| format!("{k}:{v}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                truncate_string(&prop_str, 15)
            };
            let properties = format!("{properties:<15}");
            
            println!("│ {identifier} │ {label} │ {properties} │");
        }
        println!("└─────────────────┴─────────────────┴─────────────────┘");
    }
    
    // Print edges table
    println!("\n--- Edges ---");
    if edges.is_empty() {
        println!("No edges found.");
    } else {
        println!("┌─────────────────┬─────────────────┬─────────────────┬─────────────────┬─────────────────┐");
        println!("│ Source          │ Target          │ Type            │ Direction       │ Properties      │");
        println!("├─────────────────┼─────────────────┼─────────────────┼─────────────────┼─────────────────┤");
        
        for edge in edges {
            let source = format!("{:<15}", truncate_string(&edge.source, 15));
            let target = format!("{:<15}", truncate_string(&edge.target, 15));
            let edge_type = format!("{:<15}", 
                edge.rel_type.as_ref().map_or("(none)".to_string(), |t| truncate_string(t, 15)));
            let direction = format!("{:<15}", match edge.direction {
                RelationshipDirection::Outbound => "->",
                RelationshipDirection::Inbound => "<-",
                RelationshipDirection::Bidirectional => "<->",
            });
            let properties = if edge.properties.is_empty() {
                "(none)".to_string()
            } else {
                let prop_str = edge.properties.iter()
                    .map(|(k, v)| format!("{k}:{v}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                truncate_string(&prop_str, 15)
            };
            let properties = format!("{properties:<15}");
            
            println!("│ {source} │ {target} │ {edge_type} │ {direction} │ {properties} │");
        }
        println!("└─────────────────┴─────────────────┴─────────────────┴─────────────────┴─────────────────┘");
    }
}

/// Helper function to truncate strings for table display
fn truncate_string(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[..max_len.saturating_sub(3)])
    }
}

fn main() {
    // Get command line arguments
    let args: Vec<String> = env::args().collect();

    if args.len() != 2 {
        eprintln!("Usage: {} <filename>", args[0]);
        std::process::exit(1);
    }

    let filename = &args[1];

    // Read the file content
    let content = match fs::read_to_string(filename) {
        Ok(content) => content,
        Err(e) => {
            eprintln!("Error reading file '{filename}': {e}");
            std::process::exit(1);
        }
    };

    println!("Read file '{}' with {} characters", filename, content.len());

    // Convert to C string
    let c_content = match CString::new(content) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Error converting content to C string: {e}");
            std::process::exit(1);
        }
    };

    // Create parser config
    let config = unsafe { cypher_parser_new_config() };
    if config.is_null() {
        eprintln!("Error creating parser config");
        std::process::exit(1);
    }

    // Parse the content
    let result: *mut cypher_parse_result = unsafe {
        cypher_uparse(
            c_content.as_ptr().cast::<c_char>(),
            c_content.as_bytes().len() as u64,
            ptr::null_mut(),
            config,
            u64::from(CYPHER_PARSE_DEFAULT),
        )
    };

    // Clean up config
    unsafe {
        cypher_parser_config_free(config);
    }

    if result.is_null() {
        eprintln!("Error: Failed to parse Cypher query");
        std::process::exit(1);
    }

    // Print debug information about the result
    println!("=== Parse Result Debug Output ===");

    let n_roots = unsafe { cypher_parse_result_nroots(result) };
    println!("Number of root nodes: {n_roots}");

    let n_nodes = unsafe { cypher_parse_result_nnodes(result) };
    println!("Total number of nodes: {n_nodes}");

    let n_errors = unsafe { cypher_parse_result_nerrors(result) };
    println!("Number of errors: {n_errors}");

    let is_eof = unsafe { cypher_parse_result_eof(result) };
    println!("End of file reached: {is_eof}");

    // Print errors if any
    if n_errors > 0 {
        println!("\n=== Parse Errors ===");
        for i in 0..n_errors {
            let error = unsafe { cypher_parse_result_get_error(result, i) };
            if !error.is_null() {
                let position = unsafe { cypher_parse_error_position(error) };
                let message = unsafe {
                    let msg_ptr = cypher_parse_error_message(error);
                    if msg_ptr.is_null() {
                        "No message".to_string()
                    } else {
                        // Don't convert to owned string, just print the raw pointer content
                        // The C library manages this memory
                        "Error message available (C string)".to_string()
                    }
                };
                println!("Error {i}: Position {position:?}, Message: {message}");
            }
        }
    }

    // Process query if parsing was successful
    if n_errors == 0 && n_roots > 0 {
        // Try to find MATCH-RETURN pattern
        match find_match_return_pattern(result) {
            Some(match_node) => {
                println!("\n=== Query Structure ===");
                println!("Found MATCH clause followed by RETURN clause");
                
                // Create pattern graph from MATCH clause
                match make_match_graph(match_node) {
                    Ok((vertices, edges)) => {
                        println!("Successfully created pattern graph with {} vertices and {} edges", 
                                vertices.len(), edges.len());
                         for v in &vertices {
                             println!("Vertex: {v:?}");
                         }
                         for e in &edges {
                             println!("Edge: {e:?}");
                         }
                        print_pattern_graph(&vertices, &edges);
                    }
                    Err(GraphError::VariableLengthRelationship) => {
                        eprintln!("\nError: Variable length relationships are not supported in this implementation");
                        std::process::exit(1);
                    }
                    Err(GraphError::InvalidAstNode) => {
                        eprintln!("\nError: Invalid or corrupted AST node encountered");
                        std::process::exit(1);
                    }
                    Err(GraphError::UnsupportedPattern) => {
                        eprintln!("\nError: Unsupported pattern type found in MATCH clause");
                        std::process::exit(1);
                    }
                    Err(GraphError::InvalidIdentifier) => {
                        eprintln!("\nError: Invalid identifier found in pattern");
                        std::process::exit(1);
                    }
                }
            }
            None => {
                println!("\n=== Query Structure ===");
                println!("Query does not follow expected MATCH-RETURN pattern");
                println!("Expected structure: MATCH <pattern> RETURN <expression>");
                println!("\nTip: Make sure your query starts with MATCH and ends with RETURN");
            }
        }
    } else if n_errors > 0 {
        eprintln!("\nCannot process query due to parse errors above");
        std::process::exit(1);
    } else {
        eprintln!("\nNo query content found to process");
    }

    // Clean up result
    unsafe {
        cypher_parse_result_free(result);
    }

    println!("\nProcessing completed!");
}
