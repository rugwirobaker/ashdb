# AshDB Interpreter + CLI/REPL Implementation Plan

## Overview
Implement a CLI/REPL interpreter for AshDB that supports SET, GET, SCAN commands with for-loop semantics, bridging between human-readable inputs and the byte-based LSM store.

## SCAN API Design

### Core SCAN Operations
- **Prefix Scan**: `SCAN user:` - returns all keys starting with "user:"
- **Range Scan**: `SCAN start..end` - returns keys in lexicographic range
- **Single Key Scan**: `SCAN user:123` - returns key and any keys with that prefix

### For-Loop Integration
```
FOR id IN SCAN user: { SET $id.status "active" }
```
1. `SCAN user:` yields iterator over keys: `["user:123", "user:124"]`
2. Each iteration: `id = "user:123"`, then `$id.status = "user:123.status"`
3. Execute: `SET "user:123.status" "active"`
4. Creates hierarchical key structure using lexicographic ordering

### Key Features
- **Variable Substitution**: `$variable` expansion in for-loop bodies
- **Hierarchical Keys**: Dot notation creates related keys (user:123.status)
- **Prefix Inheritance**: `SCAN user:123` returns both `user:123` and `user:123.status`
- **Order Preservation**: Uses existing keycode.rs encoding for consistent ordering

## Architecture Components

### 1. Command Parser (`src/cli/parser.rs`)
- Parse text commands: `SET key value`, `GET key`, `SCAN prefix`, `SCAN start..end`
- Support for-loop syntax: `FOR key IN SCAN prefix { SET $key.suffix value }`
- Handle different value types: strings, integers, floats, booleans
- Error handling for malformed commands

### 2. Command Types (`src/cli/command.rs`)
- `SetCommand { key, value }`
- `GetCommand { key }`
- `ScanCommand { range_or_prefix }`
- `ForCommand { variable, iterator, body }`
- Support typed values using existing encoding system

### 3. CLI Engine (`src/cli/engine.rs`)
- Execute parsed commands against LsmStore
- Handle type inference and encoding/decoding
- Manage command history and error reporting
- Implement for-loop execution with variable substitution

### 4. REPL Interface (`src/cli/repl.rs`)
- Interactive prompt with line editing
- Command completion and history
- Pretty-print results with hierarchical key display
- Batch mode for script execution

### 5. Value System (`src/cli/value.rs`)
- `CliValue` enum (String, Integer, Float, Boolean, Bytes)
- Automatic type inference from user input
- Integration with existing `Key` and `Value` traits
- Display formatting for human-readable output

## Implementation Steps

1. **Create CLI module structure** in `src/cli/`
2. **Implement command parser** with regex/nom parsing
3. **Create command execution engine** using existing Store trait
4. **Build REPL with rustyline** for interactive experience
5. **Add for-loop interpreter** with variable substitution
6. **Integrate with main.rs** replacing placeholder
7. **Add comprehensive tests** for all command types
8. **Update Cargo.toml** with CLI dependencies (rustyline, nom/regex)

## Dependencies to Add
- `rustyline` - REPL line editing and history
- `nom` or `regex` - Command parsing
- `clap` - CLI argument parsing for batch mode

## Command Examples
```
> SET user:123 "John Doe"
> GET user:123
"John Doe"
> SET user:124 "Jane Smith"
> SCAN user:
user:123 = "John Doe"
user:124 = "Jane Smith"
> FOR id IN SCAN user: { SET $id.status "active" }
> SCAN user:123
user:123 = "John Doe"
user:123.status = "active"
```

This plan leverages the existing robust LSM store and encoding system while providing an intuitive interface for database operations with powerful SCAN-based iteration capabilities.