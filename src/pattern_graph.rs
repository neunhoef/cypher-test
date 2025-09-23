use libcypher_parser_sys::cypher_rel_direction::*;
use libcypher_parser_sys::*;
use serde_json::Value;
use std::collections::{HashMap, HashSet, VecDeque};
use std::ffi::CStr;

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

/// Represents a pattern path, which can either be a proper path with edges or a single vertex
#[derive(Debug, Clone, PartialEq)]
pub enum PatternPath {
    /// A proper path with one or more edges (sequence of edge indices)
    ProperPath(Vec<usize>),
    /// A vertex-only path containing just a single vertex (vertex index)
    VertexPath(usize),
}

/// Represents a collection of named pattern paths
#[derive(Debug, Clone, PartialEq)]
pub struct PatternPaths {
    /// Internal mapping from path names to pattern paths
    paths: HashMap<String, PatternPath>,
}

impl PatternPaths {
    /// Creates a new empty PatternPaths collection
    pub fn new() -> Self {
        Self {
            paths: HashMap::new(),
        }
    }

    /// Inserts a new pattern path with the given name
    #[allow(dead_code)]
    pub fn insert(&mut self, name: String, path: PatternPath) {
        self.paths.insert(name, path);
    }

    /// Gets a pattern path by name
    #[allow(dead_code)]
    pub fn get(&self, name: &str) -> Option<&PatternPath> {
        self.paths.get(name)
    }

    /// Gets a mutable reference to a pattern path by name
    #[allow(dead_code)]
    pub fn get_mut(&mut self, name: &str) -> Option<&mut PatternPath> {
        self.paths.get_mut(name)
    }

    /// Returns an iterator over all path names and their corresponding paths
    pub fn iter(&self) -> impl Iterator<Item = (&String, &PatternPath)> {
        self.paths.iter()
    }

    /// Returns true if the collection is empty
    pub fn is_empty(&self) -> bool {
        self.paths.is_empty()
    }

    /// Returns the number of paths in the collection
    pub fn len(&self) -> usize {
        self.paths.len()
    }

    /// Converts to the internal HashMap for compatibility
    #[allow(dead_code)]
    pub fn into_inner(self) -> HashMap<String, PatternPath> {
        self.paths
    }

    /// Gets a reference to the internal HashMap for compatibility
    #[allow(dead_code)]
    pub fn as_inner(&self) -> &HashMap<String, PatternPath> {
        &self.paths
    }
}

impl Default for PatternPaths {
    fn default() -> Self {
        Self::new()
    }
}

/// Represents a complete pattern graph with vertices, edges, and named paths
#[derive(Debug, Clone, PartialEq)]
pub struct PatternGraph {
    /// Collection of pattern vertices (nodes)
    pub vertices: Vec<PatternVertex>,
    /// Collection of pattern edges (relationships)
    pub edges: Vec<PatternEdge>,
    /// Named pattern paths mapping to edge indices
    pub paths: PatternPaths,
}

/// Direction for AQL graph traversal
#[derive(Debug, Clone, PartialEq)]
pub enum TraversalDirection {
    Outbound, // OUTBOUND
    Inbound,  // INBOUND
    Any,      // ANY (for bidirectional edges)
}

/// Index structure for efficient graph traversal
/// Maps each vertex index to its neighbors in different directions
#[derive(Debug, Clone)]
pub struct EdgeIndex {
    /// Vector of outgoing neighbors for each vertex (indexed by vertex index)
    #[allow(dead_code)]
    pub outgoing: Vec<Vec<usize>>,
    /// Vector of incoming neighbors for each vertex (indexed by vertex index)
    #[allow(dead_code)]
    pub incoming: Vec<Vec<usize>>,
    /// Vector of all undirected neighbors for each vertex (union of incoming and outgoing)
    pub undirected: Vec<Vec<usize>>,
}

impl EdgeIndex {
    /// Create a new EdgeIndex from vertices and edges
    pub fn new(vertices: &[PatternVertex], edges: &[PatternEdge]) -> Self {
        let n_vertices = vertices.len();
        let mut outgoing: Vec<Vec<usize>> = vec![Vec::new(); n_vertices];
        let mut incoming: Vec<Vec<usize>> = vec![Vec::new(); n_vertices];
        let mut undirected: Vec<Vec<usize>> = vec![Vec::new(); n_vertices];

        // Create mapping from vertex identifier to index
        let mut id_to_index: HashMap<String, usize> = HashMap::new();
        for (index, vertex) in vertices.iter().enumerate() {
            id_to_index.insert(vertex.identifier.clone(), index);
        }

        // Process each edge to build neighbor lists
        for edge in edges {
            let source_idx = match id_to_index.get(&edge.source) {
                Some(idx) => *idx,
                None => continue, // Skip edges referencing non-existent vertices
            };
            let target_idx = match id_to_index.get(&edge.target) {
                Some(idx) => *idx,
                None => continue, // Skip edges referencing non-existent vertices
            };

            match edge.direction {
                RelationshipDirection::Outbound => {
                    // source -> target
                    outgoing[source_idx].push(target_idx);
                    incoming[target_idx].push(source_idx);

                    // For undirected connectivity, add both directions
                    undirected[source_idx].push(target_idx);
                    undirected[target_idx].push(source_idx);
                }
                RelationshipDirection::Inbound => {
                    // target -> source (inbound from source perspective)
                    outgoing[target_idx].push(source_idx);
                    incoming[source_idx].push(target_idx);

                    // For undirected connectivity, add both directions
                    undirected[source_idx].push(target_idx);
                    undirected[target_idx].push(source_idx);
                }
                RelationshipDirection::Bidirectional => {
                    // Both directions
                    outgoing[source_idx].push(target_idx);
                    outgoing[target_idx].push(source_idx);
                    incoming[source_idx].push(target_idx);
                    incoming[target_idx].push(source_idx);

                    // For undirected connectivity, add both directions
                    undirected[source_idx].push(target_idx);
                    undirected[target_idx].push(source_idx);
                }
            }
        }

        EdgeIndex {
            outgoing,
            incoming,
            undirected,
        }
    }
}

/// Represents an edge in the spanning tree with direction information
#[derive(Debug, Clone)]
pub struct SpanningTreeEdge {
    /// Index of the source vertex (already visited)
    pub from_vertex: usize,
    /// Index of the target vertex (newly discovered)
    pub to_vertex: usize,
    /// Reference to the original pattern edge
    pub edge_index: usize,
    /// Direction to traverse this edge (OUTBOUND, INBOUND, or ANY)
    pub traversal_direction: TraversalDirection,
}

impl PatternGraph {
    /// Creates a new empty pattern graph
    pub fn new() -> Self {
        Self {
            vertices: Vec::new(),
            edges: Vec::new(),
            paths: PatternPaths::new(),
        }
    }

    /// Creates a pattern graph with the given components
    pub fn from_components(
        vertices: Vec<PatternVertex>,
        edges: Vec<PatternEdge>,
        paths: PatternPaths,
    ) -> Self {
        Self {
            vertices,
            edges,
            paths,
        }
    }

    /// Adds a vertex to the graph
    #[allow(dead_code)]
    pub fn add_vertex(&mut self, vertex: PatternVertex) {
        self.vertices.push(vertex);
    }

    /// Adds an edge to the graph
    #[allow(dead_code)]
    pub fn add_edge(&mut self, edge: PatternEdge) {
        self.edges.push(edge);
    }

    /// Adds a named pattern path
    #[allow(dead_code)]
    pub fn add_path(&mut self, name: String, path: PatternPath) {
        self.paths.insert(name, path);
    }

    /// Gets a vertex by its identifier
    #[allow(dead_code)]
    pub fn get_vertex_by_id(&self, id: &str) -> Option<&PatternVertex> {
        self.vertices.iter().find(|v| v.identifier == id)
    }

    /// Gets an edge by its identifier
    #[allow(dead_code)]
    pub fn get_edge_by_id(&self, id: &str) -> Option<&PatternEdge> {
        self.edges.iter().find(|e| e.identifier == id)
    }

    /// Gets edges for a specific path name
    #[allow(dead_code)]
    pub fn get_path_edges(&self, path_name: &str) -> Option<Vec<&PatternEdge>> {
        self.paths.get(path_name).map(|path| {
            match path {
                PatternPath::ProperPath(indices) => {
                    indices.iter().map(|&i| &self.edges[i]).collect()
                }
                PatternPath::VertexPath(_) => {
                    // Vertex-only path has no edges
                    vec![]
                }
            }
        })
    }

    /// Returns the number of vertices in the graph
    pub fn vertex_count(&self) -> usize {
        self.vertices.len()
    }

    /// Returns the number of edges in the graph
    pub fn edge_count(&self) -> usize {
        self.edges.len()
    }

    /// Returns the number of named paths in the graph
    pub fn path_count(&self) -> usize {
        self.paths.len()
    }

    /// Returns true if the graph is empty (no vertices or edges)
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.vertices.is_empty() && self.edges.is_empty()
    }

    /// Converts to the old tuple format for backward compatibility
    /// Note: This method only works with ProperPath variants and will panic on VertexPath variants
    #[allow(dead_code)]
    pub fn into_tuple(
        self,
    ) -> (
        Vec<PatternVertex>,
        Vec<PatternEdge>,
        HashMap<String, Vec<usize>>,
    ) {
        let mut old_paths = HashMap::new();
        for (name, path) in self.paths.paths {
            match path {
                PatternPath::ProperPath(edges) => {
                    old_paths.insert(name, edges);
                }
                PatternPath::VertexPath(_) => {
                    panic!(
                        "Cannot convert VertexPath to old tuple format - use new PatternPath enum instead"
                    );
                }
            }
        }
        (self.vertices, self.edges, old_paths)
    }

    /// Creates a PatternGraph from the old tuple format
    #[allow(dead_code)]
    pub fn from_tuple(
        tuple: (
            Vec<PatternVertex>,
            Vec<PatternEdge>,
            HashMap<String, Vec<usize>>,
        ),
    ) -> Self {
        let (vertices, edges, path_map) = tuple;
        let mut converted_paths = HashMap::new();
        for (name, edge_indices) in path_map {
            converted_paths.insert(name, PatternPath::ProperPath(edge_indices));
        }
        let paths = PatternPaths {
            paths: converted_paths,
        };
        Self {
            vertices,
            edges,
            paths,
        }
    }

    /// Creates an EdgeIndex for this pattern graph
    pub fn create_edge_index(&self) -> EdgeIndex {
        EdgeIndex::new(&self.vertices, &self.edges)
    }

    /// Check if the pattern graph is connected when viewed as an undirected graph
    pub fn is_connected(&self) -> bool {
        let edge_index = self.create_edge_index();

        // Empty graph is considered connected
        if self.vertices.is_empty() {
            return true;
        }

        // Single vertex is connected
        if self.vertices.len() == 1 {
            return true;
        }

        // Use breadth-first search starting from the first vertex (index 0)
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();

        // Start BFS from vertex index 0
        queue.push_back(0);
        visited.insert(0);

        while let Some(current_vertex_idx) = queue.pop_front() {
            // Get undirected neighbors of the current vertex
            for &neighbor_idx in &edge_index.undirected[current_vertex_idx] {
                if !visited.contains(&neighbor_idx) {
                    visited.insert(neighbor_idx);
                    queue.push_back(neighbor_idx);
                }
            }
        }

        // The graph is connected if we visited all vertices
        visited.len() == self.vertices.len()
    }

    /// Build a spanning tree using breadth-first search from a starting vertex
    /// Returns a list of edges in the spanning tree in the order they should be traversed
    ///
    /// # Arguments
    /// * `start_vertex` - Index of the starting vertex for BFS
    ///
    /// # Returns
    /// * `Result<Vec<SpanningTreeEdge>, String>` - Ordered list of spanning tree edges or error
    pub fn build_spanning_tree(
        &self,
        start_vertex: usize,
    ) -> Result<Vec<SpanningTreeEdge>, String> {
        if start_vertex >= self.vertices.len() {
            return Err("Start vertex index out of bounds".to_string());
        }

        let edge_index = self.create_edge_index();
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        let mut spanning_tree_edges = Vec::new();

        // Create mapping from vertex identifier to index
        let mut id_to_index: HashMap<String, usize> = HashMap::new();
        for (index, vertex) in self.vertices.iter().enumerate() {
            id_to_index.insert(vertex.identifier.clone(), index);
        }

        // Create reverse mapping from vertex pair to edge index and direction info
        let mut edge_lookup: HashMap<(usize, usize), (usize, TraversalDirection)> = HashMap::new();
        for (edge_idx, edge) in self.edges.iter().enumerate() {
            let source_idx = match id_to_index.get(&edge.source) {
                Some(idx) => *idx,
                None => continue,
            };
            let target_idx = match id_to_index.get(&edge.target) {
                Some(idx) => *idx,
                None => continue,
            };

            match edge.direction {
                RelationshipDirection::Outbound => {
                    // source -> target, so traversal is OUTBOUND from source to target
                    edge_lookup.insert(
                        (source_idx, target_idx),
                        (edge_idx, TraversalDirection::Outbound),
                    );
                    // For reverse direction (target to source), traversal is INBOUND
                    edge_lookup.insert(
                        (target_idx, source_idx),
                        (edge_idx, TraversalDirection::Inbound),
                    );
                }
                RelationshipDirection::Inbound => {
                    // source <- target, so traversal is INBOUND from target to source
                    edge_lookup.insert(
                        (target_idx, source_idx),
                        (edge_idx, TraversalDirection::Outbound),
                    );
                    // For reverse direction (source to target), traversal is INBOUND
                    edge_lookup.insert(
                        (source_idx, target_idx),
                        (edge_idx, TraversalDirection::Inbound),
                    );
                }
                RelationshipDirection::Bidirectional => {
                    // Both directions are ANY
                    edge_lookup.insert(
                        (source_idx, target_idx),
                        (edge_idx, TraversalDirection::Any),
                    );
                    edge_lookup.insert(
                        (target_idx, source_idx),
                        (edge_idx, TraversalDirection::Any),
                    );
                }
            }
        }

        // Start BFS from the specified vertex
        queue.push_back(start_vertex);
        visited.insert(start_vertex);

        while let Some(current_vertex_idx) = queue.pop_front() {
            // Explore undirected neighbors
            for &neighbor_idx in &edge_index.undirected[current_vertex_idx] {
                if !visited.contains(&neighbor_idx) {
                    visited.insert(neighbor_idx);
                    queue.push_back(neighbor_idx);

                    // Find the edge information for this traversal
                    if let Some((edge_idx, traversal_direction)) =
                        edge_lookup.get(&(current_vertex_idx, neighbor_idx))
                    {
                        spanning_tree_edges.push(SpanningTreeEdge {
                            from_vertex: current_vertex_idx,
                            to_vertex: neighbor_idx,
                            edge_index: *edge_idx,
                            traversal_direction: traversal_direction.clone(),
                        });
                    }
                }
            }
        }

        Ok(spanning_tree_edges)
    }
}

impl Default for PatternGraph {
    fn default() -> Self {
        Self::new()
    }
}

/// Type alias for the return value of make_match_graph
pub type MatchGraphResult = PatternGraph;

/// Creates a graph representation from a MATCH statement AST
pub fn make_match_graph(
    match_node: *const cypher_astnode_t,
) -> Result<MatchGraphResult, GraphError> {
    if match_node.is_null() {
        return Err(GraphError::InvalidAstNode);
    }

    let match_type = unsafe { CYPHER_AST_MATCH };

    // Verify this is a MATCH node
    let node_type = unsafe { cypher_astnode_type(match_node) };
    if node_type != match_type {
        return Err(GraphError::UnsupportedPattern);
    }

    // Initialize collections for vertices and edges
    let mut vertices: Vec<PatternVertex> = Vec::new();
    let mut edges: Vec<PatternEdge> = Vec::new();
    let mut node_counter = 0u32;
    let mut rel_counter = 0usize;
    let mut path_counter = 0usize;
    let mut path_edge_mapping: HashMap<String, PatternPath> = HashMap::new();

    // Get the pattern from the MATCH statement (should be the first child)
    let n_children = unsafe { cypher_astnode_nchildren(match_node) };
    if n_children == 0 {
        return Ok(PatternGraph::new());
    }

    let pattern = unsafe { cypher_astnode_get_child(match_node, 0) };
    process_pattern(
        pattern,
        &mut vertices,
        &mut edges,
        &mut node_counter,
        &mut rel_counter,
        &mut path_counter,
        &mut path_edge_mapping,
    )?;

    let paths = PatternPaths {
        paths: path_edge_mapping,
    };
    Ok(PatternGraph::from_components(vertices, edges, paths))
}

/// Processes a pattern node and extracts vertices and edges
fn process_pattern(
    pattern: *const cypher_astnode_t,
    vertices: &mut Vec<PatternVertex>,
    edges: &mut Vec<PatternEdge>,
    node_counter: &mut u32,
    rel_counter: &mut usize,
    path_counter: &mut usize,
    path_edge_mapping: &mut HashMap<String, PatternPath>,
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
            process_pattern(
                child,
                vertices,
                edges,
                node_counter,
                rel_counter,
                path_counter,
                path_edge_mapping,
            )?;
        }
    } else if pattern_type == cypher_named_path {
        // Named path - get the path name from the first child and actual path from the second child
        let n_children = unsafe { cypher_astnode_nchildren(pattern) };
        if n_children >= 2 {
            let path_name_node = unsafe { cypher_astnode_get_child(pattern, 0) };
            let path = unsafe { cypher_astnode_get_child(pattern, 1) };

            // Extract the path name
            let path_name = extract_identifier(path_name_node).unwrap_or_else(|| {
                *path_counter += 1;
                format!("path{}", *path_counter)
            });

            // Process the pattern path and collect edge indices
            let edges_start_index = edges.len();
            let vertices_start_index = vertices.len();
            process_pattern_path(path, vertices, edges, node_counter, rel_counter)?;
            let edges_end_index = edges.len();
            let vertices_end_index = vertices.len();

            // Determine if this is a vertex-only path or a proper path with edges
            if edges_start_index == edges_end_index {
                // No edges were added - this is a vertex-only path
                // We need to find the vertex that was referenced in this pattern
                let vertex_index = if vertices_start_index < vertices_end_index {
                    // A vertex was added, use the last added vertex
                    vertices_end_index - 1
                } else {
                    // No new vertex was added, so we need to find the existing vertex
                    // This happens when the same vertex appears in multiple paths
                    find_vertex_in_pattern(path, vertices)?
                };
                path_edge_mapping.insert(path_name, PatternPath::VertexPath(vertex_index));
            } else {
                // Edges were added - this is a proper path
                let edge_indices: Vec<usize> = (edges_start_index..edges_end_index).collect();
                path_edge_mapping.insert(path_name, PatternPath::ProperPath(edge_indices));
            }
        }
    } else if pattern_type == cypher_pattern_path {
        // Anonymous path - invent a name
        *path_counter += 1;
        let path_name = format!("path{}", *path_counter);

        let edges_start_index = edges.len();
        let vertices_start_index = vertices.len();
        process_pattern_path(pattern, vertices, edges, node_counter, rel_counter)?;
        let edges_end_index = edges.len();
        let vertices_end_index = vertices.len();

        // Determine if this is a vertex-only path or a proper path with edges
        if edges_start_index == edges_end_index {
            // No edges were added - this is a vertex-only path
            // We need to find the vertex that was referenced in this pattern
            let vertex_index = if vertices_start_index < vertices_end_index {
                // A vertex was added, use the last added vertex
                vertices_end_index - 1
            } else {
                // No new vertex was added, so we need to find the existing vertex
                // This happens when the same vertex appears in multiple paths
                find_vertex_in_pattern(pattern, vertices)?
            };
            path_edge_mapping.insert(path_name, PatternPath::VertexPath(vertex_index));
        } else {
            // Edges were added - this is a proper path
            let edge_indices: Vec<usize> = (edges_start_index..edges_end_index).collect();
            path_edge_mapping.insert(path_name, PatternPath::ProperPath(edge_indices));
        }
    } else {
        // Try to process as pattern path anyway with invented name
        *path_counter += 1;
        let path_name = format!("path{}", *path_counter);

        let edges_start_index = edges.len();
        let vertices_start_index = vertices.len();
        process_pattern_path(pattern, vertices, edges, node_counter, rel_counter)?;
        let edges_end_index = edges.len();
        let vertices_end_index = vertices.len();

        // Determine if this is a vertex-only path or a proper path with edges
        if edges_start_index == edges_end_index {
            // No edges were added - this is a vertex-only path
            // We need to find the vertex that was referenced in this pattern
            let vertex_index = if vertices_start_index < vertices_end_index {
                // A vertex was added, use the last added vertex
                vertices_end_index - 1
            } else {
                // No new vertex was added, so we need to find the existing vertex
                // This happens when the same vertex appears in multiple paths
                find_vertex_in_pattern(pattern, vertices)?
            };
            path_edge_mapping.insert(path_name, PatternPath::VertexPath(vertex_index));
        } else {
            // Edges were added - this is a proper path
            let edge_indices: Vec<usize> = (edges_start_index..edges_end_index).collect();
            path_edge_mapping.insert(path_name, PatternPath::ProperPath(edge_indices));
        }
    }

    Ok(())
}

/// Helper function to ensure a vertex exists in the vertices collection
/// Returns the identifier of the vertex (either found or newly created)
fn ensure_vertex_exists(vertices: &mut Vec<PatternVertex>, vertex: PatternVertex) -> String {
    let identifier = vertex.identifier.clone();

    // Check if vertex with this identifier already exists
    if !vertices.iter().any(|v| v.identifier == identifier) {
        vertices.push(vertex);
    }

    identifier
}

/// Helper function to find the vertex index for a vertex-only pattern
/// This is used when the pattern contains only a single vertex that already exists
fn find_vertex_in_pattern(
    pattern_path: *const cypher_astnode_t,
    vertices: &[PatternVertex],
) -> Result<usize, GraphError> {
    if pattern_path.is_null() {
        return Err(GraphError::InvalidAstNode);
    }

    let n_children = unsafe { cypher_astnode_nchildren(pattern_path) };
    let cypher_node_pattern = unsafe { CYPHER_AST_NODE_PATTERN };

    // Look for the first (and should be only) node pattern
    for i in 0..n_children {
        let child = unsafe { cypher_astnode_get_child(pattern_path, i) };
        let child_type = unsafe { cypher_astnode_type(child) };

        if child_type == cypher_node_pattern {
            // Extract the vertex identifier
            let vertex_identifier =
                if let Some(id) = extract_vertex_identifier_from_node_pattern(child) {
                    id
                } else {
                    return Err(GraphError::InvalidIdentifier);
                };

            // Find the vertex in the vertices list
            for (index, vertex) in vertices.iter().enumerate() {
                if vertex.identifier == vertex_identifier {
                    return Ok(index);
                }
            }

            return Err(GraphError::InvalidIdentifier);
        }
    }

    Err(GraphError::UnsupportedPattern)
}

/// Helper function to extract vertex identifier from a node pattern
fn extract_vertex_identifier_from_node_pattern(node: *const cypher_astnode_t) -> Option<String> {
    if node.is_null() {
        return None;
    }

    let n_children = unsafe { cypher_astnode_nchildren(node) };
    let cypher_identifier = unsafe { CYPHER_AST_IDENTIFIER };

    for i in 0..n_children {
        let child = unsafe { cypher_astnode_get_child(node, i) };
        if child.is_null() {
            continue;
        }

        let child_type = unsafe { cypher_astnode_type(child) };
        if child_type == cypher_identifier {
            return extract_identifier(child);
        }
    }

    None
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
            current_node_id = Some(ensure_vertex_exists(vertices, vertex));
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
                        let edge = process_relationship_pattern(
                            child,
                            source_id.clone(),
                            target_id,
                            rel_counter,
                        )?;
                        edges.push(edge);

                        current_node_id = Some(ensure_vertex_exists(vertices, target_vertex));

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
pub fn extract_identifier(node: *const cypher_astnode_t) -> Option<String> {
    if node.is_null() {
        return None;
    }

    // Verify this is actually an identifier node
    let node_type = unsafe { cypher_astnode_type(node) };
    let cypher_identifier = unsafe { CYPHER_AST_IDENTIFIER };
    if node_type != cypher_identifier {
        return None;
    }

    let identifier_ptr = unsafe { cypher_ast_identifier_get_name(node) };
    if identifier_ptr.is_null() {
        return None;
    }

    let c_str = unsafe { CStr::from_ptr(identifier_ptr) };
    match c_str.to_str() {
        Ok(s) => Some(s.to_string()),
        Err(_) => None,
    }
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

    let label_ptr = unsafe { cypher_ast_label_get_name(node) };
    if label_ptr.is_null() {
        return None;
    }

    let c_str = unsafe { CStr::from_ptr(label_ptr) };
    match c_str.to_str() {
        Ok(s) => Some(s.to_string()),
        Err(_) => None,
    }
}

/// Extracts relationship type text from a reltype AST node
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

    let reltype_ptr = unsafe { cypher_ast_reltype_get_name(node) };
    if reltype_ptr.is_null() {
        return None;
    }

    let c_str = unsafe { CStr::from_ptr(reltype_ptr) };
    match c_str.to_str() {
        Ok(s) => Some(s.to_string()),
        Err(_) => None,
    }
}

/// Extracts properties from a map AST node and returns them as a HashMap
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
    let n_entries = unsafe { cypher_ast_map_nentries(node) };

    for i in 0..n_entries {
        // Get the key and value for this entry directly from the map
        let key_node = unsafe { cypher_ast_map_get_key(node, i) };
        let value_node = unsafe { cypher_ast_map_get_value(node, i) };

        if let Some(key) = extract_property_name(key_node) {
            if let Some(value) = extract_expression_value(value_node) {
                properties.insert(key, value);
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

    let name_ptr = unsafe { cypher_ast_prop_name_get_value(node) };
    if name_ptr.is_null() {
        return None;
    }

    let c_str = unsafe { CStr::from_ptr(name_ptr) };
    match c_str.to_str() {
        Ok(s) => Some(s.to_string()),
        Err(_) => None,
    }
}

/// Extracts the value from various expression AST nodes
fn extract_expression_value(node: *const cypher_astnode_t) -> Option<Value> {
    if node.is_null() {
        return None;
    }

    let node_type = unsafe { cypher_astnode_type(node) };
    let cypher_string = unsafe { CYPHER_AST_STRING };
    let cypher_integer = unsafe { CYPHER_AST_INTEGER };
    let cypher_float = unsafe { CYPHER_AST_FLOAT };
    let cypher_boolean = unsafe { CYPHER_AST_BOOLEAN };
    let cypher_true = unsafe { CYPHER_AST_TRUE };
    let cypher_false = unsafe { CYPHER_AST_FALSE };
    let cypher_null = unsafe { CYPHER_AST_NULL };

    if node_type == cypher_string {
        let str_ptr = unsafe { cypher_ast_string_get_value(node) };
        if !str_ptr.is_null() {
            let c_str = unsafe { CStr::from_ptr(str_ptr) };
            if let Ok(s) = c_str.to_str() {
                return Some(Value::String(s.to_string()));
            }
        }
    } else if node_type == cypher_integer {
        let int_ptr = unsafe { cypher_ast_integer_get_valuestr(node) };
        if !int_ptr.is_null() {
            let c_str = unsafe { CStr::from_ptr(int_ptr) };
            if let Ok(s) = c_str.to_str() {
                if let Ok(i) = s.parse::<i64>() {
                    return Some(Value::Number(i.into()));
                }
            }
        }
    } else if node_type == cypher_float {
        let float_ptr = unsafe { cypher_ast_float_get_valuestr(node) };
        if !float_ptr.is_null() {
            let c_str = unsafe { CStr::from_ptr(float_ptr) };
            if let Ok(s) = c_str.to_str() {
                if let Ok(f) = s.parse::<f64>() {
                    if let Some(num) = serde_json::Number::from_f64(f) {
                        return Some(Value::Number(num));
                    }
                }
            }
        }
    } else if node_type == cypher_boolean || node_type == cypher_true || node_type == cypher_false {
        // For boolean nodes, check the specific type
        if node_type == cypher_true {
            return Some(Value::Bool(true));
        } else if node_type == cypher_false {
            return Some(Value::Bool(false));
        } else {
            // For generic boolean nodes, default to false
            return Some(Value::Bool(false));
        }
    } else if node_type == cypher_null {
        return Some(Value::Null);
    }

    None
}

/// Extracts depth information from variable length relationship patterns
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
        if child_type == cypher_range {
            return extract_range_values(child);
        }
    }

    (Some(1), Some(1)) // Default if no range found
}

/// Extracts min and max values from a CYPHER_AST_RANGE node
fn extract_range_values(range_node: *const cypher_astnode_t) -> (Option<u32>, Option<u32>) {
    if range_node.is_null() {
        return (None, None);
    }

    let start_node = unsafe { cypher_ast_range_get_start(range_node) };
    let end_node = unsafe { cypher_ast_range_get_end(range_node) };

    let min_depth = if start_node.is_null() {
        None
    } else {
        extract_integer_from_node(start_node)
    };

    let max_depth = if end_node.is_null() {
        None
    } else {
        extract_integer_from_node(end_node)
    };

    (min_depth, max_depth)
}

/// Extracts integer value from an AST node
fn extract_integer_from_node(node: *const cypher_astnode_t) -> Option<u32> {
    if node.is_null() {
        return None;
    }

    let node_type = unsafe { cypher_astnode_type(node) };
    let cypher_integer = unsafe { CYPHER_AST_INTEGER };

    if node_type == cypher_integer {
        let int_ptr = unsafe { cypher_ast_integer_get_valuestr(node) };
        if !int_ptr.is_null() {
            let c_str = unsafe { CStr::from_ptr(int_ptr) };
            if let Ok(s) = c_str.to_str() {
                if let Ok(i) = s.parse::<u32>() {
                    return Some(i);
                }
            }
        }
    }

    None
}

/// Determines the direction of a relationship pattern based on its AST structure
fn determine_relationship_direction(rel: *const cypher_astnode_t) -> RelationshipDirection {
    if rel.is_null() {
        return RelationshipDirection::Outbound; // Default fallback
    }

    // Check the relationship pattern node type first
    let node_type = unsafe { cypher_astnode_type(rel) };
    let cypher_rel_pattern = unsafe { CYPHER_AST_REL_PATTERN };
    if node_type != cypher_rel_pattern {
        return RelationshipDirection::Outbound; // Default for non-relationship patterns
    }

    // Use the libcypher-parser function to get direction
    let direction = unsafe { cypher_ast_rel_pattern_get_direction(rel) };

    // Map libcypher-parser direction constants to our enum
    match direction {
        CYPHER_REL_OUTBOUND => RelationshipDirection::Outbound,
        CYPHER_REL_INBOUND => RelationshipDirection::Inbound,
        CYPHER_REL_BIDIRECTIONAL => RelationshipDirection::Bidirectional,
    }
}

/// Prints the pattern graph in list format
pub fn print_pattern_graph(graph: &PatternGraph) {
    println!("\n=== Pattern Graph ===");

    // Print vertices list
    println!("\n--- Vertices ---");
    if graph.vertices.is_empty() {
        println!("No vertices found.");
    } else {
        for vertex in &graph.vertices {
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
    if graph.edges.is_empty() {
        println!("No edges found.");
    } else {
        for edge in &graph.edges {
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
                        format!(
                            "Edge: {} -[{}:{}{}]-> {}",
                            edge.source, edge.identifier, rel_type, depth_info, edge.target
                        )
                    }
                    RelationshipDirection::Inbound => {
                        format!(
                            "Edge: {} <-[{}:{}{}]- {}",
                            edge.source, edge.identifier, rel_type, depth_info, edge.target
                        )
                    }
                    RelationshipDirection::Bidirectional => {
                        format!(
                            "Edge: {} -[{}:{}{}]- {}",
                            edge.source, edge.identifier, rel_type, depth_info, edge.target
                        )
                    }
                }
            } else {
                match edge.direction {
                    RelationshipDirection::Outbound => {
                        format!(
                            "Edge: {} -[{}{}]-> {}",
                            edge.source, edge.identifier, depth_info, edge.target
                        )
                    }
                    RelationshipDirection::Inbound => {
                        format!(
                            "Edge: {} <-[{}{}]- {}",
                            edge.source, edge.identifier, depth_info, edge.target
                        )
                    }
                    RelationshipDirection::Bidirectional => {
                        format!(
                            "Edge: {} -[{}{}]- {}",
                            edge.source, edge.identifier, depth_info, edge.target
                        )
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
#[path = "tests_pattern_graph.rs"]
mod tests_pattern_graph;
