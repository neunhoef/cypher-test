# Using libcypher-parser in Rust

This guide explains how to use the `libcypher-parser-sys` crate to parse Cypher queries and work with their Abstract Syntax Trees (AST) in Rust programs.

## Table of Contents

1. [Setup and Dependencies](#setup-and-dependencies)
2. [Basic Parsing](#basic-parsing)
3. [Error Handling](#error-handling)
4. [AST Navigation](#ast-navigation)
5. [Complete Example](#complete-example)
6. [Advanced Usage](#advanced-usage)
7. [Memory Management](#memory-management)
8. [Best Practices](#best-practices)

## Setup and Dependencies

Add the `libcypher-parser-sys` dependency to your `Cargo.toml`:

```toml
[dependencies]
libcypher-parser-sys = "0.6.0"
```

The crate provides Rust bindings for the C library `libcypher-parser`, which generates bindings automatically during compilation.

## Basic Parsing

### 1. Include the Bindings

```rust
use std::ffi::CString;
use std::os::raw::c_char;
use std::ptr;

// Import all necessary functions and types
#[allow(clippy::wildcard_imports)]
use libcypher_parser_sys::*;
```

### 2. Parse a Cypher Query

```rust
fn parse_cypher_query(query: &str) -> Result<*const cypher_parse_result_t, String> {
    // Convert Rust string to C string
    let c_query = CString::new(query)
        .map_err(|e| format!("Invalid query string: {}", e))?;
    
    // Create parser configuration
    let config = unsafe { cypher_parser_new_config() };
    if config.is_null() {
        return Err("Failed to create parser config".to_string());
    }
    
    // Parse the query
    let result = unsafe {
        cypher_uparse(
            c_query.as_ptr().cast::<c_char>(),
            c_query.as_bytes().len() as u64,
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
        Err("Failed to parse query".to_string())
    } else {
        Ok(result)
    }
}
```

## Error Handling

The parser provides detailed error information when parsing fails:

```rust
fn check_parse_errors(result: *const cypher_parse_result_t) {
    let n_errors = unsafe { cypher_parse_result_nerrors(result) };
    
    if n_errors > 0 {
        println!("Parse failed with {} errors:", n_errors);
        
        for i in 0..n_errors {
            let error = unsafe { cypher_parse_result_get_error(result, i) };
            if !error.is_null() {
                let position = unsafe { cypher_parse_error_position(error) };
                println!("Error {}: Line {}, Column {}, Offset {}", 
                        i, position.line, position.column, position.offset);
                
                // Note: Error messages are C strings managed by the library
                // Don't try to convert them to Rust strings to avoid double-free
                println!("Error message available (C string)");
            }
        }
    }
}
```

## AST Navigation

### 1. Basic AST Structure

The AST is a tree structure where each node has:
- A type (represented as a `u8`)
- Zero or more child nodes
- Optional data (strings, numbers, etc.)

### 2. Key Functions for AST Navigation

```rust
// Get the number of root nodes in the parse result
let n_roots = unsafe { cypher_parse_result_nroots(result) };

// Get a specific root node
let root = unsafe { cypher_parse_result_get_root(result, 0) };

// Get node type
let node_type = unsafe { cypher_astnode_type(node) };

// Get human-readable type name
let type_str = unsafe {
    let type_str_ptr = cypher_astnode_typestr(node_type);
    if type_str_ptr.is_null() {
        "Unknown".to_string()
    } else {
        CStr::from_ptr(type_str_ptr.cast::<c_char>())
            .to_string_lossy()
            .to_string()
    }
};

// Get number of children
let n_children = unsafe { cypher_astnode_nchildren(node) };

// Get a specific child node
let child = unsafe { cypher_astnode_get_child(node, index) };
```

### 3. Recursive AST Traversal

```rust
fn print_ast(node: *const cypher_astnode_t, indent: usize) {
    if node.is_null() {
        println!("{}<null>", "  ".repeat(indent));
        return;
    }

    let node_type = unsafe { cypher_astnode_type(node) };
    let type_str = unsafe {
        let type_str_ptr = cypher_astnode_typestr(node_type);
        if type_str_ptr.is_null() {
            format!("Unknown({})", node_type)
        } else {
            CStr::from_ptr(type_str_ptr.cast::<c_char>())
                .to_string_lossy()
                .to_string()
        }
    };
    let n_children = unsafe { cypher_astnode_nchildren(node) };
    
    println!("{}{} (type: {}, children: {})", 
             "  ".repeat(indent), type_str, node_type, n_children);

    // Recursively print children
    for i in 0..n_children {
        let child = unsafe { cypher_astnode_get_child(node, i) };
        print_ast(child, indent + 1);
    }
}
```

## Complete Example

Here's a complete working example that parses a Cypher query and prints its AST:

```rust
use std::env;
use std::ffi::{CString, CStr};
use std::os::raw::c_char;
use std::ptr;

#[allow(clippy::wildcard_imports)]
use libcypher_parser_sys::*;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} <query>", args[0]);
        std::process::exit(1);
    }
    
    let query = &args[1];
    
    // Parse the query
    let result = match parse_query(query) {
        Ok(result) => result,
        Err(e) => {
            eprintln!("Parse error: {}", e);
            std::process::exit(1);
        }
    };
    
    // Check for errors
    let n_errors = unsafe { cypher_parse_result_nerrors(result) };
    if n_errors > 0 {
        println!("Parse failed with {} errors", n_errors);
        check_parse_errors(result);
    } else {
        println!("Parse successful!");
        print_ast_structure(result);
    }
    
    // Clean up
    unsafe {
        cypher_parse_result_free(result);
    }
}

fn parse_query(query: &str) -> Result<*const cypher_parse_result_t, String> {
    let c_query = CString::new(query)
        .map_err(|e| format!("Invalid query: {}", e))?;
    
    let config = unsafe { cypher_parser_new_config() };
    if config.is_null() {
        return Err("Failed to create parser config".to_string());
    }
    
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
    
    if result.is_null() {
        Err("Failed to parse query".to_string())
    } else {
        Ok(result)
    }
}

fn print_ast_structure(result: *const cypher_parse_result_t) {
    let n_roots = unsafe { cypher_parse_result_nroots(result) };
    println!("=== AST Structure ({} root nodes) ===", n_roots);
    
    for i in 0..n_roots {
        let root = unsafe { cypher_parse_result_get_root(result, i) };
        if !root.is_null() {
            println!("\n--- Root Node {} ---", i);
            print_ast_node(root, 0);
        }
    }
}

fn print_ast_node(node: *const cypher_astnode_t, indent: usize) {
    if node.is_null() {
        println!("{}<null>", "  ".repeat(indent));
        return;
    }

    let node_type = unsafe { cypher_astnode_type(node) };
    let type_str = unsafe {
        let type_str_ptr = cypher_astnode_typestr(node_type);
        if type_str_ptr.is_null() {
            format!("Unknown({})", node_type)
        } else {
            CStr::from_ptr(type_str_ptr.cast::<c_char>())
                .to_string_lossy()
                .to_string()
        }
    };
    let n_children = unsafe { cypher_astnode_nchildren(node) };
    
    println!("{}{} (type: {}, children: {})", 
             "  ".repeat(indent), type_str, node_type, n_children);

    for i in 0..n_children {
        let child = unsafe { cypher_astnode_get_child(node, i) };
        print_ast_node(child, indent + 1);
    }
}

fn check_parse_errors(result: *const cypher_parse_result_t) {
    let n_errors = unsafe { cypher_parse_result_nerrors(result) };
    
    for i in 0..n_errors {
        let error = unsafe { cypher_parse_result_get_error(result, i) };
        if !error.is_null() {
            let position = unsafe { cypher_parse_error_position(error) };
            println!("Error {}: Line {}, Column {}, Offset {}", 
                    i, position.line, position.column, position.offset);
        }
    }
}
```

## Advanced Usage

### 1. Working with Specific Node Types

You can check for specific node types and extract information:

```rust
fn extract_identifiers(node: *const cypher_astnode_t) -> Vec<String> {
    let mut identifiers = Vec::new();
    
    if node.is_null() {
        return identifiers;
    }
    
    let node_type = unsafe { cypher_astnode_type(node) };
    
    // Check if this is an identifier node
    if node_type == CYPHER_AST_IDENTIFIER {
        // Extract identifier value (this would require additional C functions)
        identifiers.push("identifier".to_string());
    }
    
    // Recursively check children
    let n_children = unsafe { cypher_astnode_nchildren(node) };
    for i in 0..n_children {
        let child = unsafe { cypher_astnode_get_child(node, i) };
        identifiers.extend(extract_identifiers(child));
    }
    
    identifiers
}
```

### 2. Pattern Matching on Node Types

```rust
fn analyze_node(node: *const cypher_astnode_t) -> String {
    if node.is_null() {
        return "null".to_string();
    }
    
    let node_type = unsafe { cypher_astnode_type(node) };
    
    match node_type {
        CYPHER_AST_MATCH => "MATCH clause".to_string(),
        CYPHER_AST_RETURN => "RETURN clause".to_string(),
        CYPHER_AST_CREATE => "CREATE clause".to_string(),
        CYPHER_AST_DELETE => "DELETE clause".to_string(),
        CYPHER_AST_SET => "SET clause".to_string(),
        CYPHER_AST_REMOVE => "REMOVE clause".to_string(),
        CYPHER_AST_MERGE => "MERGE clause".to_string(),
        CYPHER_AST_WITH => "WITH clause".to_string(),
        CYPHER_AST_UNWIND => "UNWIND clause".to_string(),
        CYPHER_AST_CALL => "CALL clause".to_string(),
        CYPHER_AST_LOAD_CSV => "LOAD CSV clause".to_string(),
        CYPHER_AST_FOREACH => "FOREACH clause".to_string(),
        CYPHER_AST_START => "START clause".to_string(),
        CYPHER_AST_UNION => "UNION clause".to_string(),
        CYPHER_AST_PATTERN => "Pattern".to_string(),
        CYPHER_AST_NODE_PATTERN => "Node pattern".to_string(),
        CYPHER_AST_REL_PATTERN => "Relationship pattern".to_string(),
        CYPHER_AST_EXPRESSION => "Expression".to_string(),
        CYPHER_AST_IDENTIFIER => "Identifier".to_string(),
        CYPHER_AST_STRING => "String literal".to_string(),
        CYPHER_AST_INTEGER => "Integer literal".to_string(),
        CYPHER_AST_FLOAT => "Float literal".to_string(),
        CYPHER_AST_BOOLEAN => "Boolean literal".to_string(),
        CYPHER_AST_TRUE => "True".to_string(),
        CYPHER_AST_FALSE => "False".to_string(),
        CYPHER_AST_NULL => "Null".to_string(),
        CYPHER_AST_LABEL => "Label".to_string(),
        CYPHER_AST_RELTYPE => "Relationship type".to_string(),
        CYPHER_AST_PROP_NAME => "Property name".to_string(),
        CYPHER_AST_FUNCTION_NAME => "Function name".to_string(),
        CYPHER_AST_PARAMETER => "Parameter".to_string(),
        CYPHER_AST_MAP => "Map".to_string(),
        CYPHER_AST_COLLECTION => "Collection".to_string(),
        _ => format!("Unknown node type: {}", node_type),
    }
}
```

## Memory Management

### Important Notes

1. **C Library Memory Management**: The C library manages memory for parse results and error messages. Don't try to free these manually.

2. **Always Free Parse Results**: Always call `cypher_parse_result_free()` when you're done with a parse result.

3. **Don't Convert Error Messages**: Error message pointers are managed by the C library. Converting them to Rust strings can cause double-free errors.

4. **Null Pointer Checks**: Always check for null pointers before dereferencing.

```rust
// Correct memory management
fn safe_parse_and_analyze(query: &str) -> Result<(), String> {
    let result = parse_query(query)?;
    
    // Use the result...
    analyze_ast(result);
    
    // Always clean up
    unsafe {
        cypher_parse_result_free(result);
    }
    
    Ok(())
}
```

## Best Practices

### 1. Error Handling
- Always check for null pointers
- Use proper error propagation with `Result` types
- Don't ignore parse errors

### 2. Code Organization
- Create wrapper functions for common operations
- Use modules to organize AST-related functionality
- Document your functions thoroughly

### 3. Performance
- Reuse parser configurations when possible
- Avoid unnecessary string conversions
- Consider caching parsed results for repeated queries

### 4. Safety
- Use `unsafe` blocks only when necessary
- Validate all inputs before passing to C functions
- Follow Rust ownership rules carefully

## Common Node Types

Here are some of the most commonly encountered AST node types:

- **`CYPHER_AST_QUERY`**: Root query node
- **`CYPHER_AST_MATCH`**: MATCH clause
- **`CYPHER_AST_RETURN`**: RETURN clause
- **`CYPHER_AST_CREATE`**: CREATE clause
- **`CYPHER_AST_DELETE`**: DELETE clause
- **`CYPHER_AST_SET`**: SET clause
- **`CYPHER_AST_REMOVE`**: REMOVE clause
- **`CYPHER_AST_MERGE`**: MERGE clause
- **`CYPHER_AST_PATTERN`**: Pattern (nodes and relationships)
- **`CYPHER_AST_NODE_PATTERN`**: Individual node pattern
- **`CYPHER_AST_REL_PATTERN`**: Relationship pattern
- **`CYPHER_AST_EXPRESSION`**: Expression
- **`CYPHER_AST_IDENTIFIER`**: Variable or identifier
- **`CYPHER_AST_STRING`**: String literal
- **`CYPHER_AST_INTEGER`**: Integer literal
- **`CYPHER_AST_FLOAT`**: Float literal
- **`CYPHER_AST_BOOLEAN`**: Boolean literal
- **`CYPHER_AST_LABEL`**: Node or relationship label
- **`CYPHER_AST_PROP_NAME`**: Property name
- **`CYPHER_AST_FUNCTION_NAME`**: Function name
- **`CYPHER_AST_PARAMETER`**: Query parameter

## Conclusion

The `libcypher-parser-sys` crate provides a powerful way to parse and analyze Cypher queries in Rust. By following the patterns shown in this guide, you can build robust applications that work with Cypher query ASTs while maintaining memory safety and good performance.

Remember to always handle errors gracefully, manage memory properly, and validate your inputs. The C library is powerful but requires careful attention to these details when used from Rust.
