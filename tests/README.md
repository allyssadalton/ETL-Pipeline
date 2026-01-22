# ETL Pipeline Test Suite

This directory contains comprehensive test cases for the ETL Pipeline project.

## Test Structure

- `test_ingestion.py` - Tests for data ingestion functions
- `test_transformation.py` - Tests for data transformation functions
- `test_validation.py` - Tests for data validation functions and rules
- `test_storage.py` - Tests for database operations
- `test_analytics.py` - Tests for quality metrics and reporting
- `test_integration.py` - Integration tests for the full pipeline

## Running Tests

### Prerequisites
Install test dependencies:
```bash
pip install -r requirements.txt
```

### Run All Tests
```bash
pytest tests/
```

### Run Specific Test File
```bash
pytest tests/test_transformation.py
```

### Run with Verbose Output
```bash
pytest tests/ -v
```

### Run Tests with Coverage
```bash
pytest tests/ --cov=ingestion --cov=transformation --cov=validation --cov=storage --cov=analytics
```

## Test Coverage

The test suite covers:

### Ingestion Tests
- Logging setup
- Client configuration loading
- Mapping configuration loading
- CSV file reading
- Main ingestion pipeline

### Transformation Tests
- Date format conversion
- Field mapping
- Status code normalization
- Date normalization
- Metadata addition
- Full transformation pipeline

### Validation Tests
- Individual record validation
- Schema validation rules
- Bulk record validation
- Validation rule functions (required fields, numbers, dates, allowed values)

### Storage Tests
- Database table creation
- Clean record insertion
- Rejected record insertion
- Error handling for invalid data

### Analytics Tests
- Quality metrics computation
- Rejection rate calculation
- Business reporting
- Status distribution analysis

### Integration Tests
- End-to-end pipeline testing
- Component interaction validation
- Full workflow simulation

## Test Data

Tests use mock data and temporary files/databases to avoid affecting production data. The test suite includes:

- Sample CSV data files
- Mock client configurations
- Test schemas and mappings
- Temporary SQLite databases for storage tests

## Adding New Tests

When adding new functionality:

1. Create corresponding test functions in the appropriate test file
2. Follow the naming convention: `test_<functionality>`
3. Use descriptive test names that explain what is being tested
4. Include both positive and negative test cases
5. Mock external dependencies where appropriate
6. Test edge cases and error conditions

## CI/CD Integration

These tests can be integrated into CI/CD pipelines to ensure code quality:

```yaml
# Example GitHub Actions
- name: Run Tests
  run: |
    pip install -r requirements.txt
    pytest tests/ --cov-report=xml
```

## Test Maintenance

- Keep tests updated when code changes
- Remove obsolete tests
- Ensure tests run quickly
- Use fixtures for common test data setup