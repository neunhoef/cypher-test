use crate::cypher::{ReturnClause, ReturnProjection};
use crate::pattern_graph::{
    PatternEdge, PatternGraph, PatternVertex, SpanningTreeEdge, TraversalDirection,
};
use serde_json::Value;
use std::collections::HashMap;

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
        .map(|(key, value)| match value {
            Value::String(s) => format!("{variable_name}.{key} == '{s}'"),
            Value::Number(n) => format!("{variable_name}.{key} == {n}"),
            Value::Bool(b) => format!("{variable_name}.{key} == {b}"),
            Value::Null => format!("{variable_name}.{key} == null"),
            _ => format!("{variable_name}.{key} == {value}"),
        })
        .collect();

    conditions.join(" AND ")
}

/// Generate FILTER conditions for multi-hop edge properties
/// Returns a string with all property conditions using the path syntax
///
/// # Arguments
/// * `properties` - Property map with key-value pairs
/// * `path_variable` - Name of the path variable (e.g., "_p_e")
///
/// # Returns
/// * `String` with FILTER conditions using path edges syntax, empty if no properties
fn generate_multi_hop_edge_filter_conditions(properties: &HashMap<String, Value>, path_variable: &str) -> String {
    if properties.is_empty() {
        return String::new();
    }

    let conditions: Vec<String> = properties
        .iter()
        .map(|(key, value)| match value {
            Value::String(s) => format!("{path_variable}.edges[*].{key} ALL == '{s}'"),
            Value::Number(n) => format!("{path_variable}.edges[*].{key} ALL == {n}"),
            Value::Bool(b) => format!("{path_variable}.edges[*].{key} ALL == {b}"),
            Value::Null => format!("{path_variable}.edges[*].{key} ALL == null"),
            _ => format!("{path_variable}.edges[*].{key} ALL == {value}"),
        })
        .collect();

    conditions.join(" AND ")
}


/// Generate PRUNE conditions for vertex properties in multi-hop traversals
/// Returns a string with PRUNE condition for optimization
/// Uses inverted logic: prunes when edge is not null AND any property doesn't match
///
/// # Arguments
/// * `vertex` - The target vertex with properties
/// * `vertex_identifier` - The identifier of the vertex variable
/// * `edge_identifier` - The identifier of the edge variable (empty if none)
///
/// # Returns
/// * `String` with PRUNE condition, empty if no properties
fn generate_vertex_prune_conditions(vertex: &PatternVertex, vertex_identifier: &str, edge_identifier: &str) -> String {
    if vertex.properties.is_empty() {
        return String::new();
    }

    let vertex_conditions: Vec<String> = vertex.properties
        .iter()
        .map(|(key, value)| match value {
            Value::String(s) => format!("{vertex_identifier}.{key} != '{s}'"),
            Value::Number(n) => format!("{vertex_identifier}.{key} != {n}"),
            Value::Bool(b) => format!("{vertex_identifier}.{key} != {b}"),
            Value::Null => format!("{vertex_identifier}.{key} != null"),
            _ => format!("{vertex_identifier}.{key} != {value}"),
        })
        .collect();

    if vertex_conditions.is_empty() {
        return String::new();
    }

    let vertex_condition_part = vertex_conditions.join(" || ");
    
    if edge_identifier.is_empty() {
        // No edge variable, just the inverted vertex conditions
        vertex_condition_part
    } else {
        // Include the inverted edge null check with AND
        format!("{edge_identifier} != null && ({vertex_condition_part})")
    }
}

/// Derive edge collection name from relationship type or use default
///
/// # Arguments
/// * `edge` - The pattern edge to derive collection name for
///
/// # Returns
/// * Collection name string, or error if no rel_type specified
fn derive_edge_collection_name(edge: &PatternEdge, config: &crate::config::Config) -> String {
    let rel_type_str = edge.rel_type.as_deref();
    config.get_edge_reference(rel_type_str)
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
    config: &crate::config::Config,
) -> Result<AQLLine, String> {
    let edge = &edges[spanning_edge.edge_index];
    let from_vertex = &vertices[spanning_edge.from_vertex];
    let to_vertex = &vertices[spanning_edge.to_vertex];

    // Get edge collection name or graph reference
    let edge_collection = derive_edge_collection_name(edge, config);

    // Determine AQL direction keyword
    let direction_keyword = match spanning_edge.traversal_direction {
        TraversalDirection::Outbound => "OUTBOUND",
        TraversalDirection::Inbound => "INBOUND",
        TraversalDirection::Any => "ANY",
    };

    // Generate FOR statement with proper depth range
    let min_depth = edge.min_depth.unwrap_or(1);
    let max_depth = edge.max_depth.unwrap_or(1);
    let depth_range = format!("{min_depth}..{max_depth}");
    
    // Check if this is a multi-hop edge (not 1..1)
    let is_multi_hop = min_depth != 1 || max_depth != 1;
    
    let for_statement = if edge.identifier.is_empty() {
        // No edge variable needed
        if is_multi_hop {
            // Multi-hop without edge variable - just vertex and path
            format!(
                "FOR {}, _p_{} IN {} {} {}._id {}",
                to_vertex.identifier, to_vertex.identifier, depth_range, direction_keyword, from_vertex.identifier, edge_collection
            )
        } else {
            // Single-hop without edge variable
            format!(
                "FOR {} IN {} {} {}._id {}",
                to_vertex.identifier, depth_range, direction_keyword, from_vertex.identifier, edge_collection
            )
        }
    } else {
        // Include edge variable
        if is_multi_hop {
            // Multi-hop with edge variable - vertex, edge, and path
            format!(
                "FOR {}, {}, _p_{} IN {} {} {}._id {}",
                to_vertex.identifier,
                edge.identifier,
                edge.identifier,
                depth_range,
                direction_keyword,
                from_vertex.identifier,
                edge_collection
            )
        } else {
            // Single-hop with edge variable
            format!(
                "FOR {}, {} IN {} {} {}._id {}",
                to_vertex.identifier,
                edge.identifier,
                depth_range,
                direction_keyword,
                from_vertex.identifier,
                edge_collection
            )
        }
    };

    let current_indent = *indent;
    *indent += 1; // Increase indentation for subsequent statements

    // Generate exposed variables list
    let mut exposed_vars = vec![to_vertex.identifier.clone()];
    if !edge.identifier.is_empty() {
        exposed_vars.push(edge.identifier.clone());
        if is_multi_hop {
            // Also expose the path identifier for multi-hop edges
            exposed_vars.push(format!("_p_{}", edge.identifier));
        }
    } else if is_multi_hop {
        // For multi-hop edges without edge identifier, expose path with vertex identifier
        exposed_vars.push(format!("_p_{}", to_vertex.identifier));
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

    // Check if this is a multi-hop edge
    let min_depth = edge.min_depth.unwrap_or(1);
    let max_depth = edge.max_depth.unwrap_or(1);
    let is_multi_hop = min_depth != 1 || max_depth != 1;

    let filter_conditions = if is_multi_hop {
        // Use path-based filtering for multi-hop edges
        let path_variable = format!("_p_{}", edge.identifier);
        generate_multi_hop_edge_filter_conditions(&edge.properties, &path_variable)
    } else {
        // Use regular filtering for single-hop edges
        generate_filter_conditions(&edge.properties, &edge.identifier)
    };

    if filter_conditions.is_empty() {
        return None;
    }

    Some(AQLLine {
        content: format!("FILTER {filter_conditions}"),
        indent,
        exposed_variables: vec![], // FILTER statements don't expose new variables
    })
}

/// Generate FILTER conditions for vertex properties
/// Now uses simple vertex property syntax for both single-hop and multi-hop cases
///
/// # Arguments
/// * `vertex` - The target vertex with properties
/// * `edge` - The edge used to reach this vertex (currently unused, kept for compatibility)
/// * `indent` - Current indentation level
///
/// # Returns
/// * `Option<AQLLine>` - FILTER line if vertex has properties, None otherwise
pub fn generate_vertex_filter_for_multi_hop(vertex: &PatternVertex, _edge: &PatternEdge, indent: usize) -> Option<AQLLine> {
    if vertex.properties.is_empty() {
        return None;
    }

    // Use simple vertex property filtering for all cases (both single-hop and multi-hop)
    let filter_conditions = generate_filter_conditions(&vertex.properties, &vertex.identifier);
    if filter_conditions.is_empty() {
        return None;
    }

    Some(AQLLine {
        content: format!("FILTER {filter_conditions}"),
        indent,
        exposed_variables: vec![], // FILTER statements don't expose new variables
    })
}

/// Generate PRUNE statement for vertex properties in multi-hop traversals
///
/// # Arguments
/// * `vertex` - The target vertex with properties
/// * `edge` - The edge used to reach this vertex (to determine if multi-hop)
/// * `indent` - Current indentation level
///
/// # Returns
/// * `Option<AQLLine>` - PRUNE line if vertex has properties and edge is multi-hop, None otherwise
pub fn generate_vertex_prune_for_multi_hop(vertex: &PatternVertex, edge: &PatternEdge, indent: usize) -> Option<AQLLine> {
    if vertex.properties.is_empty() {
        return None;
    }

    // Check if this is a multi-hop edge
    let min_depth = edge.min_depth.unwrap_or(1);
    let max_depth = edge.max_depth.unwrap_or(1);
    let is_multi_hop = min_depth != 1 || max_depth != 1;

    if !is_multi_hop {
        // No PRUNE needed for single-hop edges
        return None;
    }

    // Generate PRUNE conditions
    let prune_conditions = generate_vertex_prune_conditions(vertex, &vertex.identifier, &edge.identifier);
    if prune_conditions.is_empty() {
        return None;
    }

    Some(AQLLine {
        content: format!("PRUNE {prune_conditions}"),
        indent,
        exposed_variables: vec![], // PRUNE statements don't expose new variables
    })
}

/// Derive collection name from vertex label or use default
///
/// # Arguments
/// * `vertex` - The vertex to derive collection name for
///
/// # Returns
/// * Collection name string
fn derive_collection_name(vertex: &PatternVertex, config: &crate::config::Config) -> String {
    let label_str = vertex.label.as_deref();
    config.get_vertex_collection(label_str)
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
pub fn match_to_aql(pattern_graph: &PatternGraph, config: &crate::config::Config) -> Result<(Vec<AQLLine>, usize), String> {
    let vertices = &pattern_graph.vertices;
    let edges = &pattern_graph.edges;
    if vertices.is_empty() {
        return Err("No vertices in pattern graph".to_string());
    }

    // Multi-hop edges are now supported

    // Find the anchor vertex (most properties, smallest index for ties)
    let anchor_index =
        find_anchor_vertex(vertices).ok_or("Failed to find anchor vertex".to_string())?;

    let anchor_vertex = &vertices[anchor_index];
    let collection_name = derive_collection_name(anchor_vertex, config);
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
            let traversal_line =
                generate_edge_traversal(spanning_edge, vertices, edges, &mut current_indent, config)?;
            aql_lines.push(traversal_line);

            // Generate PRUNE conditions for target vertex properties if this is a multi-hop edge
            let edge = &edges[spanning_edge.edge_index];
            let target_vertex = &vertices[spanning_edge.to_vertex];
            if let Some(vertex_prune) = generate_vertex_prune_for_multi_hop(target_vertex, edge, current_indent) {
                aql_lines.push(vertex_prune);
            }

            // Generate FILTER conditions for edge properties if they exist
            if let Some(edge_filter) = generate_edge_filter(edge, current_indent) {
                aql_lines.push(edge_filter);
            }

            // Generate FILTER conditions for target vertex properties if they exist
            if let Some(vertex_filter) = generate_vertex_filter_for_multi_hop(target_vertex, edge, current_indent) {
                aql_lines.push(vertex_filter);
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

/// Helper function to check if an edge is multi-hop
fn is_multi_hop_edge(edge: &crate::pattern_graph::PatternEdge) -> bool {
    let min_depth = edge.min_depth.unwrap_or(1);
    let max_depth = edge.max_depth.unwrap_or(1);
    min_depth != 1 || max_depth != 1
}

/// Helper function to get the path variable name for an edge
fn get_path_variable_name(edge: &crate::pattern_graph::PatternEdge) -> String {
    if !edge.identifier.is_empty() {
        format!("_p_{}", edge.identifier)
    } else {
        format!("_p_{}", edge.target)
    }
}

/// Generate vertices array expression for a path, handling multi-hop edges
fn generate_path_vertices_array(
    edge_indices: &[usize],
    pattern_graph: &PatternGraph,
) -> Result<String, String> {
    // Check if any edges are multi-hop to determine if we need complex processing
    let has_multi_hop = edge_indices
        .iter()
        .any(|&edge_index| is_multi_hop_edge(&pattern_graph.edges[edge_index]));

    if !has_multi_hop {
        // All edges are single-hop, use simple vertex collection
        let mut vertex_identifiers = Vec::new();
        let mut seen_vertices = std::collections::HashSet::new();

        for &edge_index in edge_indices {
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

        return Ok(format!("[{}]", vertex_identifiers.join(", ")));
    }

    // Complex processing for multi-hop edges
    let mut vertex_components = Vec::new();
    let mut seen_vertices = std::collections::HashSet::new();

    for (position, &edge_index) in edge_indices.iter().enumerate() {
        if edge_index >= pattern_graph.edges.len() {
            return Err(format!(
                "Invalid edge index {edge_index} in path"
            ));
        }

        let edge = &pattern_graph.edges[edge_index];

        // Add source vertex if not seen (for first edge only)
        if position == 0 && !seen_vertices.contains(&edge.source) {
            if is_multi_hop_edge(edge) {
                // For multi-hop edges, we need the first vertex from _p_e.vertices
                let path_var = get_path_variable_name(edge);
                vertex_components.push(format!("{path_var}.vertices[0]"));
            } else {
                // For single-hop edges, just use the vertex identifier
                vertex_components.push(edge.source.clone());
            }
            seen_vertices.insert(edge.source.clone());
        }

        // Add vertices from the edge traversal
        if is_multi_hop_edge(edge) {
            let path_var = get_path_variable_name(edge);
            // Since the first vertex of the full path is put there separately,
            // we can always remove the first vertex from _p_e.vertices
            vertex_components.push(format!("REMOVE_NTH({path_var}.vertices, 0)"));
        } else {
            // For single-hop edges, add the target vertex if not seen
            if !seen_vertices.contains(&edge.target) {
                vertex_components.push(edge.target.clone());
                seen_vertices.insert(edge.target.clone());
            }
        }
    }

    // Use FLATTEN to combine all vertex components
    if vertex_components.len() == 1 {
        Ok(vertex_components.into_iter().next().unwrap())
    } else {
        Ok(format!("FLATTEN([{}])", vertex_components.join(", ")))
    }
}

/// Generate edges array expression for a path, handling multi-hop edges
fn generate_path_edges_array(
    edge_indices: &[usize],
    pattern_graph: &PatternGraph,
) -> Result<String, String> {
    // Check if any edges are multi-hop to determine if we need complex processing
    let has_multi_hop = edge_indices
        .iter()
        .any(|&edge_index| is_multi_hop_edge(&pattern_graph.edges[edge_index]));

    if !has_multi_hop {
        // All edges are single-hop, use simple edge array
        let edge_identifiers: Vec<String> = edge_indices
            .iter()
            .map(|&edge_index| pattern_graph.edges[edge_index].identifier.clone())
            .collect();

        return Ok(format!("[{}]", edge_identifiers.join(", ")));
    }

    // Complex processing for multi-hop edges
    let mut edge_components = Vec::new();

    for &edge_index in edge_indices {
        if edge_index >= pattern_graph.edges.len() {
            return Err(format!(
                "Invalid edge index {edge_index} in path"
            ));
        }

        let edge = &pattern_graph.edges[edge_index];

        if is_multi_hop_edge(edge) {
            // For multi-hop edges, use _p_e.edges
            let path_var = get_path_variable_name(edge);
            edge_components.push(format!("{path_var}.edges"));
        } else {
            // For single-hop edges, just use the edge identifier
            edge_components.push(format!("[{}]", edge.identifier));
        }
    }

    // Use FLATTEN to combine all edge components
    if edge_components.len() == 1 {
        Ok(edge_components.into_iter().next().unwrap())
    } else {
        Ok(format!("FLATTEN([{}])", edge_components.join(", ")))
    }
}

/// Generates an AQL expression to construct a path object as JSON
/// The path object has the structure: {vertices: [...], edges: [...]}
/// For multi-hop edges, uses the _p_e.vertices and _p_e.edges arrays with proper
/// handling of vertex overlap and FLATTEN operations.
///
/// # Arguments
/// * `path_name` - Name of the path identifier
/// * `pattern_graph` - Pattern graph containing path information
///
/// # Returns
/// * `Result<String, String>` - AQL expression or error message
fn generate_path_expression(
    path_name: &str,
    pattern_graph: &PatternGraph,
) -> Result<String, String> {
    use crate::pattern_graph::PatternPath;

    // Get the path for this name
    let pattern_path = pattern_graph
        .paths
        .get(path_name)
        .ok_or_else(|| format!("Path '{path_name}' not found in pattern graph"))?;

    match pattern_path {
        PatternPath::VertexPath(vertex_index) => {
            // Single vertex path
            if *vertex_index >= pattern_graph.vertices.len() {
                return Err(format!(
                    "Invalid vertex index {vertex_index} in path '{path_name}'"
                ));
            }

            let vertex = &pattern_graph.vertices[*vertex_index];
            let vertices_array = format!("[{}]", vertex.identifier);
            let edges_array = "[]".to_string();

            Ok(format!(
                "{{\"vertices\": {vertices_array}, \"edges\": {edges_array}}}"
            ))
        }
        PatternPath::ProperPath(edge_indices) => {
            // Path with edges
            if edge_indices.is_empty() {
                return Err(format!("ProperPath '{path_name}' has no edges"));
            }

            // Generate vertices and edges arrays with multi-hop support
            let vertices_array = generate_path_vertices_array(edge_indices, pattern_graph)?;
            let edges_array = generate_path_edges_array(edge_indices, pattern_graph)?;

            Ok(format!(
                "{{\"vertices\": {vertices_array}, \"edges\": {edges_array}}}"
            ))
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
        result.push(generate_return_star(
            &exported_vars,
            return_clause.distinct,
            indent,
        ));
    } else if return_clause.projections.is_empty() {
        return Err("RETURN clause must specify either * or at least one projection".to_string());
    } else {
        // Handle specific projections
        result.push(generate_return_projections(
            &return_clause.projections,
            &exported_vars,
            pattern_graph,
            return_clause.distinct,
            indent,
        )?);
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
        let column_name = projection.alias.as_ref().unwrap_or(&projection.expression);

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
        let expression_part = if projection.is_identifier
            && pattern_graph.paths.get(&projection.expression).is_some()
        {
            // This is a path identifier - generate path expression
            generate_path_expression(&projection.expression, pattern_graph)?
        } else {
            // Regular variable or expression
            projection.expression.clone()
        };

        // Use abbreviated syntax when column name equals original expression and it's not a path
        let json_property = if column_name == &projection.expression
            && pattern_graph.paths.get(&projection.expression).is_none()
        {
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
    config: &crate::config::Config,
) -> Result<Vec<AQLLine>, String> {
    // First generate the MATCH part
    let (mut aql_lines, current_indent) = match_to_aql(pattern_graph, config)?;

    // Then generate the RETURN part using the current indentation level
    let return_lines =
        generate_return_clause(return_clause, &aql_lines, pattern_graph, current_indent)?;
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
