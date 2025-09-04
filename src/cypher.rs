use std::ffi::CStr;
use std::os::raw::c_char;
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
}
