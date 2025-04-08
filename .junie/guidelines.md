# Python Style Guide for Prefect-Lab

This document outlines the coding standards and best practices for the Prefect-Lab project.

## General Python Guidelines

### Code Style
- Follow [PEP 8](https://peps.python.org/pep-0008/) for code formatting
- Use 4 spaces for indentation (no tabs)
- Maximum line length of 88 characters (compatible with Black formatter)
- Use meaningful variable and function names that describe their purpose
- Add docstrings to all functions, classes, and modules following [PEP 257](https://peps.python.org/pep-0257/)

### Naming Conventions
- Use `snake_case` for functions, methods, and variables
- Use `PascalCase` for classes
- Use `UPPER_CASE` for constants
- Prefix private attributes with a single underscore (`_private_var`)

### Imports
- Group imports in the following order:
  1. Standard library imports
  2. Third-party library imports (including Prefect)
  3. Local application imports
- Sort imports alphabetically within each group
- Use absolute imports rather than relative imports

## Prefect-Specific Guidelines

### Flow and Task Design
- Keep tasks atomic and focused on a single responsibility
- Use meaningful names for flows and tasks that describe their purpose
- Explicitly define inputs and outputs for tasks using type hints
- Use task retries for operations that might fail temporarily
- Implement proper error handling within tasks

### Data Pipeline Patterns
- Use the `@task` decorator for individual processing steps
- Use the `@flow` decorator for orchestrating multiple tasks
- Implement caching strategies for tasks that are computationally expensive
- Use Prefect artifacts for storing metadata about task runs
- Implement proper logging at appropriate levels

### Example Flow Structure
```python
from prefect import flow, task
from typing import List, Dict, Any

@task(retries=3, retry_delay_seconds=5)
def extract_data(file_path: str) -> List[Dict[str, Any]]:
    """Extract data from a CSV file.
    
    Args:
        file_path: Path to the CSV file
        
    Returns:
        List of dictionaries containing the data
    """
    # Implementation
    pass

@task
def transform_data(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Transform the extracted data.
    
    Args:
        data: Raw data from extraction
        
    Returns:
        Transformed data
    """
    # Implementation
    pass

@task
def load_data(data: List[Dict[str, Any]], output_path: str) -> None:
    """Load data to output file.
    
    Args:
        data: Transformed data
        output_path: Path to save the output
    """
    # Implementation
    pass

@flow(name="ETL Pipeline")
def etl_pipeline(input_path: str, output_path: str) -> None:
    """Main ETL pipeline flow.
    
    Args:
        input_path: Path to input data
        output_path: Path to save output data
    """
    raw_data = extract_data(input_path)
    transformed_data = transform_data(raw_data)
    load_data(transformed_data, output_path)
```

## Project-Specific Conventions

### CSV Processing
- Use the `csv` module or `pandas` for CSV file operations
- Handle CSV headers consistently across all processing functions
- Implement proper error handling for malformed CSV data
- Document the expected CSV format in function docstrings

### Currency Data Processing
- Use consistent naming for currency pairs (e.g., `EUR_USD` instead of `EURUSD`)
- Handle decimal precision appropriately for currency values
- Use ISO date format (YYYY-MM-DD) for date fields
- Implement proper validation for currency data

## Code Organization

### Functional vs Object-Oriented
- Prefer functional programming style for data transformation tasks
- Use classes when managing state or implementing complex business logic
- Keep functions pure when possible (same input always produces same output)
- Avoid global state and mutable data structures when possible

### File Structure
- Organize code by functionality rather than by technical type
- Keep related functionality in the same module
- Use descriptive file names that reflect their purpose
- Separate utility functions into their own modules

## Common Pitfalls to Avoid

- Avoid mutable default arguments in function definitions
- Don't use wildcard imports (`from module import *`)
- Avoid deeply nested code blocks (more than 3-4 levels)
- Don't reinvent functionality that exists in standard libraries
- Avoid premature optimization
- Don't ignore exceptions without proper handling
- Avoid hardcoding paths or configuration values

## Testing Guidelines

- Write unit tests for all tasks and flows
- Use pytest as the testing framework
- Mock external dependencies in tests
- Test both success and failure scenarios
- Aim for high test coverage, especially for critical data processing logic