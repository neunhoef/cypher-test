use crate::cypher::{ReturnClause, ReturnProjection};
use crate::pattern_graph::{PatternVertex, PatternEdge, PatternGraph, TraversalDirection, SpanningTreeEdge};
use std::collections::HashMap;
use serde_json::Value;



/// Represents a line in an AQL query with indentation
#[derive(Debug, Clone, PartialEq)]
pub struct AQLLine {
    pub content: String,
    pub indent: usize,
    pub exposed_variables: Vec<String>,
}

/// Generate FILTER conditions from a property map
/// Returns a string with all property conditions connected by AND
/// 
/// # Arguments
/// * `properties` - Property map with key-value pairs
/// * `variable_name` - Name of the variable to apply conditions to
/// 
/// # Returns
/// * `String` with FILTER conditions, empty if no properties
fn generate_filter_conditions(properties: &HashMap<String, Value>, variable_name: &str) -> String {
    if properties.is_empty() {
        return String::new();
    }
    
    let conditions: Vec<String> = properties
        .iter()
        .map(|(key, value)| {
            match value {
                Value::String(s) => format!("{variable_name}.{key} == '{s}'"),
                Value::Number(n) => format!("{variable_name}.{key} == {n}"),
                Value::Bool(b) => format!("{variable_name}.{key} == {b}"),
                Value::Null => format!("{variable_name}.{key} == null"),
                _ => format!("{variable_name}.{key} == {value}"),
            }
        })
        .collect();
    
    conditions.join(" AND ")
}

/// Derive edge collection name from relationship type or use default
/// 
/// # Arguments
/// * `edge` - The pattern edge to derive collection name for
/// 
/// # Returns
/// * Collection name string, or error if no rel_type specified
fn derive_edge_collection_name(edge: &PatternEdge) -> Result<String, String> {
    match &edge.rel_type {
        Some(rel_type) => Ok(rel_type.clone()),
        None => Err("Edge type is required but not specified".to_string()),
    }
}

/// Generate AQL graph traversal statement for a spanning tree edge
/// 
/// # Arguments
/// * `spanning_edge` - The spanning tree edge to generate traversal for
/// * `vertices` - Vector of pattern vertices
/// * `edges` - Vector of pattern edges
/// * `indent` - Mutable reference to the current indentation level
/// 
/// # Returns
/// * `Result<AQLLine, String>` - AQL traversal line or error message
pub fn generate_edge_traversal(
    spanning_edge: &SpanningTreeEdge,
    vertices: &[PatternVertex],
    edges: &[PatternEdge],
    indent: &mut usize,
) -> Result<AQLLine, String> {
    let edge = &edges[spanning_edge.edge_index];
    let from_vertex = &vertices[spanning_edge.from_vertex];
    let to_vertex = &vertices[spanning_edge.to_vertex];
    
    // Get edge collection name
    let edge_collection = derive_edge_collection_name(edge)?;
    
    // Determine AQL direction keyword
    let direction_keyword = match spanning_edge.traversal_direction {
        TraversalDirection::Outbound => "OUTBOUND",
        TraversalDirection::Inbound => "INBOUND", 
        TraversalDirection::Any => "ANY",
    };
    
    // Generate FOR statement
    let for_statement = if edge.identifier.is_empty() {
        // No edge variable needed
        format!(
            "FOR {} IN 1..1 {} {}._id {}",
            to_vertex.identifier,
            direction_keyword,
            from_vertex.identifier,
            edge_collection
        )
    } else {
        // Include edge variable
        format!(
            "FOR {}, {} IN 1..1 {} {}._id {}",
            to_vertex.identifier,
            edge.identifier,
            direction_keyword,
            from_vertex.identifier,
            edge_collection
        )
    };
    
    let current_indent = *indent;
    *indent += 1; // Increase indentation for subsequent statements
    
    // Generate exposed variables list
    let mut exposed_vars = vec![to_vertex.identifier.clone()];
    if !edge.identifier.is_empty() {
        exposed_vars.push(edge.identifier.clone());
    }
    
    Ok(AQLLine {
        content: for_statement,
        indent: current_indent,
        exposed_variables: exposed_vars,
    })
}

/// Generate FILTER conditions for edge properties
/// 
/// # Arguments
/// * `edge` - The pattern edge with properties
/// * `indent` - Current indentation level
/// 
/// # Returns
/// * `Option<AQLLine>` - FILTER line if edge has properties, None otherwise
pub fn generate_edge_filter(edge: &PatternEdge, indent: usize) -> Option<AQLLine> {
    if edge.properties.is_empty() {
        return None;
    }
    
    let filter_conditions = generate_filter_conditions(&edge.properties, &edge.identifier);
    if filter_conditions.is_empty() {
        return None;
    }
    
    Some(AQLLine {
        content: format!("FILTER {filter_conditions}"),
        indent,
        exposed_variables: vec![], // FILTER statements don't expose new variables
    })
}

/// Derive collection name from vertex label or use default
/// 
/// # Arguments
/// * `vertex` - The vertex to derive collection name for
/// 
/// # Returns
/// * Collection name string
fn derive_collection_name(vertex: &PatternVertex) -> String {
    if let Some(label) = &vertex.label {
        label.clone()
    } else {
        "vertices".to_string()
    }
}

/// Find the vertex with the most prescribed properties
/// In case of a tie, select the vertex with the smallest index
/// 
/// # Arguments
/// * `vertices` - Vector of pattern vertices
/// 
/// # Returns
/// * Index of the anchor vertex, or None if vertices is empty
fn find_anchor_vertex(vertices: &[PatternVertex]) -> Option<usize> {
    if vertices.is_empty() {
        return None;
    }
    
    let mut best_index = 0;
    let mut max_properties = vertices[0].properties.len();
    
    for (index, vertex) in vertices.iter().enumerate().skip(1) {
        let prop_count = vertex.properties.len();
        if prop_count > max_properties {
            max_properties = prop_count;
            best_index = index;
        }
    }
    
    Some(best_index)
}

/// Generate AQL query from a pattern graph match statement
/// Uses breadth-first search to build a spanning tree and generates one-hop traversals
/// 
/// # Arguments
/// * `pattern_graph` - The pattern graph containing vertices and edges
/// 
/// # Returns
/// * `Result<(Vec<AQLLine>, usize), String>` with AQL lines and current indentation level, or error message
pub fn match_to_aql(pattern_graph: &PatternGraph) -> Result<(Vec<AQLLine>, usize), String> {
    let vertices = &pattern_graph.vertices;
    let edges = &pattern_graph.edges;
    if vertices.is_empty() {
        return Err("No vertices in pattern graph".to_string());
    }
    
    // Validate that all edges have constant depth 1
    for edge in edges {
        if edge.min_depth != Some(1) || edge.max_depth != Some(1) {
            return Err(format!(
                "Edge '{}' does not have constant depth 1 (min: {:?}, max: {:?}). Variable length relationships are not supported.", 
                edge.identifier, 
                edge.min_depth, 
                edge.max_depth
            ));
        }
    }
    
    // Find the anchor vertex (most properties, smallest index for ties)
    let anchor_index = find_anchor_vertex(vertices)
        .ok_or("Failed to find anchor vertex".to_string())?;
    
    let anchor_vertex = &vertices[anchor_index];
    let collection_name = derive_collection_name(anchor_vertex);
    let variable_name = &anchor_vertex.identifier;
    
    let mut aql_lines = Vec::new();
    let mut current_indent = 0; // Track current indentation level
    
    // Generate anchor FOR statement
    let for_line = AQLLine {
        content: format!("FOR {variable_name} IN {collection_name}"),
        indent: current_indent,
        exposed_variables: vec![variable_name.clone()], // Anchor exposes only the vertex identifier
    };
    aql_lines.push(for_line);
    current_indent += 1; // Increase indentation after FOR statement
    
    // Generate FILTER conditions for anchor vertex if properties exist
    let filter_conditions = generate_filter_conditions(&anchor_vertex.properties, variable_name);
    if !filter_conditions.is_empty() {
        let filter_line = AQLLine {
            content: format!("FILTER {filter_conditions}"),
            indent: current_indent,
            exposed_variables: vec![], // FILTER statements don't expose new variables
        };
        aql_lines.push(filter_line);
    }
    
    // If there are edges, build spanning tree and generate edge traversals
    if !edges.is_empty() {
        let spanning_tree = pattern_graph.build_spanning_tree(anchor_index)?;
        
        // Generate traversal statements for each edge in the spanning tree
        for spanning_edge in &spanning_tree {
            // Generate the edge traversal FOR statement
            let traversal_line = generate_edge_traversal(spanning_edge, vertices, edges, &mut current_indent)?;
            aql_lines.push(traversal_line);
            
            // Generate FILTER conditions for edge properties if they exist
            let edge = &edges[spanning_edge.edge_index];
            if let Some(edge_filter) = generate_edge_filter(edge, current_indent) {
                aql_lines.push(edge_filter);
            }
        }
    }
    
    Ok((aql_lines, current_indent))
}



/// Collects all variable names that are exported from the AQL lines
/// These are the variables that can be referenced in the RETURN clause
/// Also includes path identifiers from the pattern graph
fn collect_exported_variables(aql_lines: &[AQLLine], pattern_graph: &PatternGraph) -> Vec<String> {
    let mut exported_vars = Vec::new();
    
    for line in aql_lines {
        for var in &line.exposed_variables {
            if !exported_vars.contains(var) {
                exported_vars.push(var.clone());
            }
        }
    }
    
    // Add path identifiers as exported variables
    for (path_name, _path_edges) in pattern_graph.paths.iter() {
        if !exported_vars.contains(path_name) {
            exported_vars.push(path_name.clone());
        }
    }
    
    exported_vars
}

/// Generates an AQL expression to construct a path object as JSON
/// The path object has the structure: {vertices: [...], edges: [...]}
/// 
/// # Arguments
/// * `path_name` - Name of the path identifier
/// * `pattern_graph` - Pattern graph containing path information
/// 
/// # Returns
/// * `Result<String, String>` - AQL expression or error message
fn generate_path_expression(path_name: &str, pattern_graph: &PatternGraph) -> Result<String, String> {
    use crate::pattern_graph::PatternPath;
    
    // Get the path for this name
    let pattern_path = pattern_graph.paths.get(path_name)
        .ok_or_else(|| format!("Path '{path_name}' not found in pattern graph"))?;
    
    match pattern_path {
        PatternPath::VertexPath(vertex_index) => {
            // Single vertex path
            if *vertex_index >= pattern_graph.vertices.len() {
                return Err(format!("Invalid vertex index {vertex_index} in path '{path_name}'"));
            }
            
            let vertex = &pattern_graph.vertices[*vertex_index];
            let vertices_array = format!("[{}]", vertex.identifier);
            let edges_array = "[]".to_string();
            
            Ok(format!("{{\"vertices\": {vertices_array}, \"edges\": {edges_array}}}"))
        }
        PatternPath::ProperPath(edge_indices) => {
            // Path with edges
            if edge_indices.is_empty() {
                return Err(format!("ProperPath '{path_name}' has no edges"));
            }
            
            // Build the vertices array by collecting all vertices along the path
            let mut vertex_identifiers = Vec::new();
            let mut seen_vertices = std::collections::HashSet::new();
            
            for &edge_index in edge_indices {
                if edge_index >= pattern_graph.edges.len() {
                    return Err(format!("Invalid edge index {edge_index} in path '{path_name}'"));
                }
                
                let edge = &pattern_graph.edges[edge_index];
                
                // Add source vertex if not seen
                if !seen_vertices.contains(&edge.source) {
                    vertex_identifiers.push(edge.source.clone());
                    seen_vertices.insert(edge.source.clone());
                }
                
                // Add target vertex if not seen
                if !seen_vertices.contains(&edge.target) {
                    vertex_identifiers.push(edge.target.clone());
                    seen_vertices.insert(edge.target.clone());
                }
            }
            
            // Build the edges array
            let edge_identifiers: Vec<String> = edge_indices.iter()
                .map(|&edge_index| pattern_graph.edges[edge_index].identifier.clone())
                .collect();
            
            // Generate AQL expression
            let vertices_array = format!("[{}]", vertex_identifiers.join(", "));
            let edges_array = format!("[{}]", edge_identifiers.join(", "));
            
            Ok(format!("{{\"vertices\": {vertices_array}, \"edges\": {edges_array}}}"))
        }
    }
}

/// Generates AQL RETURN statement from a RETURN clause
/// Returns AQL lines for the RETURN statement
pub fn generate_return_clause(
    return_clause: &ReturnClause,
    aql_lines: &[AQLLine],
    pattern_graph: &PatternGraph,
    indent: usize,
) -> Result<Vec<AQLLine>, String> {
    let mut result = Vec::new();
    
    // Collect all exported variables from the query so far
    let exported_vars = collect_exported_variables(aql_lines, pattern_graph);
    
    if return_clause.return_star {
        // Handle RETURN * - return all exported variables
        result.push(generate_return_star(&exported_vars, return_clause.distinct, indent));
    } else if return_clause.projections.is_empty() {
        return Err("RETURN clause must specify either * or at least one projection".to_string());
    } else {
        // Handle specific projections
        result.push(generate_return_projections(&return_clause.projections, &exported_vars, pattern_graph, return_clause.distinct, indent)?);
    }
    
    Ok(result)
}

/// Generates AQL RETURN statement for RETURN *
fn generate_return_star(exported_vars: &[String], distinct: bool, indent: usize) -> AQLLine {
    let distinct_keyword = if distinct { "DISTINCT " } else { "" };
    
    if exported_vars.is_empty() {
        // No variables to return
        AQLLine {
            content: format!("RETURN {distinct_keyword}{{}}"),
            indent,
            exposed_variables: vec![],
        }
    } else {
        // Create JSON object with all exported variables
        let json_properties: Vec<String> = exported_vars.to_vec(); // Use abbreviated syntax {x} instead of {x: x}
        let json_object = format!("{{{}}}", json_properties.join(", "));
        
        AQLLine {
            content: format!("RETURN {distinct_keyword}{json_object}"),
            indent,
            exposed_variables: vec![],
        }
    }
}

/// Generates AQL RETURN statement for specific projections
fn generate_return_projections(
    projections: &[ReturnProjection],
    exported_vars: &[String],
    pattern_graph: &PatternGraph,
    distinct: bool,
    indent: usize,
) -> Result<AQLLine, String> {
    let distinct_keyword = if distinct { "DISTINCT " } else { "" };
    
    let mut json_properties = Vec::new();
    
    for projection in projections {
        let column_name = projection.alias.as_ref()
            .unwrap_or(&projection.expression);
        
        // Validate that the expression refers to an exported variable
        // For now, we only support simple identifier expressions
        if projection.is_identifier && !exported_vars.contains(&projection.expression) {
            return Err(format!(
                "Variable '{}' is not available in the current scope. Available variables: {}",
                projection.expression,
                exported_vars.join(", ")
            ));
        }
        
        // Check if this is a path identifier
        let expression_part = if projection.is_identifier && pattern_graph.paths.get(&projection.expression).is_some() {
            // This is a path identifier - generate path expression
            generate_path_expression(&projection.expression, pattern_graph)?
        } else {
            // Regular variable or expression
            projection.expression.clone()
        };
        
        // Use abbreviated syntax when column name equals original expression and it's not a path
        let json_property = if column_name == &projection.expression && pattern_graph.paths.get(&projection.expression).is_none() {
            column_name.clone()
        } else {
            format!("{column_name}: {expression_part}")
        };
        json_properties.push(json_property);
    }
    
    let json_object = format!("{{{}}}", json_properties.join(", "));
    
    Ok(AQLLine {
        content: format!("RETURN {distinct_keyword}{json_object}"),
        indent,
        exposed_variables: vec![],
    })
}

/// Generates a complete AQL query from MATCH and RETURN clauses
pub fn generate_complete_aql(
    pattern_graph: &PatternGraph,
    return_clause: &ReturnClause,
) -> Result<Vec<AQLLine>, String> {
    // First generate the MATCH part
    let (mut aql_lines, current_indent) = match_to_aql(pattern_graph)?;
    
    // Then generate the RETURN part using the current indentation level
    let return_lines = generate_return_clause(return_clause, &aql_lines, pattern_graph, current_indent)?;
    aql_lines.extend(return_lines);
    
    Ok(aql_lines)
}

/// Formats AQL lines into a complete query string
pub fn format_aql_query(aql_lines: &[AQLLine]) -> String {
    aql_lines
        .iter()
        .map(|line| format!("{}{}", "  ".repeat(line.indent), line.content))
        .collect::<Vec<_>>()
        .join("\n")
}

#[cfg(test)]
#[path = "tests_cypher_to_aql.rs"]
mod tests;
