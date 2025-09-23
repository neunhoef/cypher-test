use std::collections::HashMap;
use std::fs;
use std::path::Path;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Config {
    pub fall_back_for_edges: String,
    pub fall_back_graph: String,
    pub fall_back_edge_collection: String,
    pub fall_back_vertex_collection: String,
    pub mapping_label_to_vertex_collection: HashMap<String, String>,
    pub mapping_type_to_edge_collection: HashMap<String, String>,
}

impl Config {
    /// Load configuration from a JSON file
    pub fn load_from_file<P: AsRef<Path>>(config_path: P) -> Result<Config, Box<dyn std::error::Error>> {
        let content = fs::read_to_string(config_path)?;
        let config: Config = serde_json::from_str(&content)?;
        Ok(config)
    }

    /// Create a default configuration
    pub fn default() -> Config {
        Config {
            fall_back_for_edges: "collection".to_string(),
            fall_back_graph: "default_graph".to_string(),
            fall_back_edge_collection: "EDGES".to_string(),
            fall_back_vertex_collection: "VERTICES".to_string(),
            mapping_label_to_vertex_collection: HashMap::new(),
            mapping_type_to_edge_collection: HashMap::new(),
        }
    }

    /// Get the edge collection name or graph reference based on configuration
    /// Returns either a collection name or a GRAPH reference
    pub fn get_edge_reference(&self, edge_type: Option<&str>) -> String {
        if let Some(edge_type) = edge_type {
            if !edge_type.is_empty() {
                // Check mapping first
                if let Some(mapped_collection) = self.mapping_type_to_edge_collection.get(edge_type) {
                    return mapped_collection.clone();
                }
                // Use the edge type as collection name
                return edge_type.to_string();
            }
        }

        // No edge type given, use fallback
        match self.fall_back_for_edges.as_str() {
            "graph" => format!("GRAPH \"{}\"", self.fall_back_graph),
            "collection" => self.fall_back_edge_collection.clone(),
            _ => self.fall_back_edge_collection.clone(), // default to collection
        }
    }

    /// Get the vertex collection name based on configuration
    pub fn get_vertex_collection(&self, vertex_label: Option<&str>) -> String {
        if let Some(label) = vertex_label {
            if !label.is_empty() {
                // Check mapping first
                if let Some(mapped_collection) = self.mapping_label_to_vertex_collection.get(label) {
                    return mapped_collection.clone();
                }
                // Use the label as collection name
                return label.to_string();
            }
        }

        // No label given, use fallback
        self.fall_back_vertex_collection.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_edge_reference_with_mapping() {
        let mut config = Config::default();
        config.mapping_type_to_edge_collection.insert("E".to_string(), "edge_collection".to_string());
        
        assert_eq!(config.get_edge_reference(Some("E")), "edge_collection");
    }

    #[test]
    fn test_edge_reference_direct() {
        let config = Config::default();
        assert_eq!(config.get_edge_reference(Some("FOLLOWS")), "FOLLOWS");
    }

    #[test]
    fn test_edge_reference_fallback_collection() {
        let mut config = Config::default();
        config.fall_back_for_edges = "collection".to_string();
        config.fall_back_edge_collection = "EDGES".to_string();
        
        assert_eq!(config.get_edge_reference(None), "EDGES");
        assert_eq!(config.get_edge_reference(Some("")), "EDGES");
    }

    #[test]
    fn test_edge_reference_fallback_graph() {
        let mut config = Config::default();
        config.fall_back_for_edges = "graph".to_string();
        config.fall_back_graph = "security_graph".to_string();
        
        assert_eq!(config.get_edge_reference(None), "GRAPH \"security_graph\"");
        assert_eq!(config.get_edge_reference(Some("")), "GRAPH \"security_graph\"");
    }

    #[test]
    fn test_vertex_collection_with_mapping() {
        let mut config = Config::default();
        config.mapping_label_to_vertex_collection.insert("person".to_string(), "Person".to_string());
        
        assert_eq!(config.get_vertex_collection(Some("person")), "Person");
    }

    #[test]
    fn test_vertex_collection_direct() {
        let config = Config::default();
        assert_eq!(config.get_vertex_collection(Some("User")), "User");
    }

    #[test]
    fn test_vertex_collection_fallback() {
        let mut config = Config::default();
        config.fall_back_vertex_collection = "VERTICES".to_string();
        
        assert_eq!(config.get_vertex_collection(None), "VERTICES");
        assert_eq!(config.get_vertex_collection(Some("")), "VERTICES");
    }
}
