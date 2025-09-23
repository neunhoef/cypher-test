use std::env;
use std::ffi::CString;
use std::fs;
use std::os::raw::c_char;
use std::ptr;

// Use the libcypher_parser_sys crate
#[allow(clippy::wildcard_imports)]
use libcypher_parser_sys::*;

// Include our modules
mod cypher;
mod cypher_to_aql;
mod pattern_graph;
use cypher::{find_match_and_return_clauses, parse_return_clause};
use cypher_to_aql::{format_aql_query, generate_complete_aql};
use pattern_graph::{
    GraphError, PatternPath, RelationshipDirection, make_match_graph, print_pattern_graph,
};

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
        match find_match_and_return_clauses(result) {
            Some((match_node, return_node)) => {
                println!("\n=== Query Structure ===");
                println!("Found MATCH clause followed by RETURN clause");

                // Create pattern graph from MATCH clause
                match make_match_graph(match_node) {
                    Ok(graph) => {
                        println!(
                            "Successfully created pattern graph with {} vertices, {} edges, and {} paths",
                            graph.vertex_count(),
                            graph.edge_count(),
                            graph.path_count()
                        );

                        print_pattern_graph(&graph);

                        // Print detailed path information after the pattern graph
                        if !graph.paths.is_empty() {
                            println!("\n=== Pattern Paths ===");
                            for (path_name, pattern_path) in graph.paths.iter() {
                                println!("\nPath '{path_name}':");
                                match pattern_path {
                                    PatternPath::VertexPath(vertex_index) => {
                                        if *vertex_index < graph.vertices.len() {
                                            let vertex = &graph.vertices[*vertex_index];
                                            println!("  Single vertex: {}", vertex.identifier);
                                        } else {
                                            println!("  Invalid vertex index: {vertex_index}");
                                        }
                                    }
                                    PatternPath::ProperPath(edge_indices) => {
                                        if edge_indices.is_empty() {
                                            println!("  (no edges)");
                                        } else {
                                            for &edge_idx in edge_indices {
                                                if edge_idx < graph.edges.len() {
                                                    let edge = &graph.edges[edge_idx];
                                                    let edge_label = if !edge.identifier.is_empty()
                                                    {
                                                        format!(" {}", edge.identifier)
                                                    } else {
                                                        String::new()
                                                    };
                                                    let rel_type = edge
                                                        .rel_type
                                                        .as_ref()
                                                        .map_or("", |t| t.as_str());
                                                    let direction_arrow = match edge.direction {
                                                        RelationshipDirection::Outbound => "->",
                                                        RelationshipDirection::Inbound => "<-",
                                                        RelationshipDirection::Bidirectional => {
                                                            "<->"
                                                        }
                                                    };

                                                    if edge.direction
                                                        == RelationshipDirection::Inbound
                                                    {
                                                        println!(
                                                            "  {} <-[{}:{}]- {}",
                                                            edge.target,
                                                            edge_label,
                                                            rel_type,
                                                            edge.source
                                                        );
                                                    } else {
                                                        println!(
                                                            "  {} -[{}:{}]{} {}",
                                                            edge.source,
                                                            edge_label,
                                                            rel_type,
                                                            direction_arrow,
                                                            edge.target
                                                        );
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        // Parse the RETURN clause
                        match parse_return_clause(return_node) {
                            Ok(return_clause) => {
                                println!("\n=== RETURN Clause ===");
                                if return_clause.return_star {
                                    println!("RETURN * (all exported variables)");
                                } else {
                                    println!("RETURN projections:");
                                    for projection in &return_clause.projections {
                                        let alias_str = if let Some(ref alias) = projection.alias {
                                            format!(" AS {alias}")
                                        } else {
                                            String::new()
                                        };
                                        println!("  - {}{}", projection.expression, alias_str);
                                    }
                                }
                                if return_clause.distinct {
                                    println!("DISTINCT: true");
                                }

                                // Check pattern graph connectivity
                                let connected = graph.is_connected();
                                println!(
                                    "\n=== Pattern Graph Analysis ===\n\
                                     Graph connectivity: {}",
                                    if connected {
                                        "Connected"
                                    } else {
                                        "Not connected"
                                    }
                                );

                                if !connected {
                                    eprintln!(
                                        "\nError: The pattern graph is not connected. \
                                         AQL translation requires a connected pattern graph."
                                    );
                                    std::process::exit(1);
                                }

                                // Generate complete AQL query with MATCH and RETURN
                                match generate_complete_aql(&graph, &return_clause) {
                                    Ok(aql_lines) => {
                                        println!("\n=== AQL Translation ===");
                                        println!("{}", format_aql_query(&aql_lines));
                                    }
                                    Err(e) => {
                                        eprintln!("\nError generating AQL: {e}");
                                        std::process::exit(1);
                                    }
                                }
                            }
                            Err(e) => {
                                eprintln!("\nError parsing RETURN clause: {e}");
                                std::process::exit(1);
                            }
                        }
                    }
                    Err(GraphError::VariableLengthRelationship) => {
                        eprintln!(
                            "\nError: Variable length relationships are not supported in this implementation"
                        );
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
