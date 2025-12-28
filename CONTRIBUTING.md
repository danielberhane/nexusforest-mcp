# Contributing to NexusForest MCP

Thank you for your interest in contributing to NexusForest MCP! This document provides guidelines for contributing to the project.

## Code of Conduct

By participating in this project, you agree to abide by our principles of respectful and constructive collaboration.

## How to Contribute

### Reporting Issues
- Check if the issue already exists
- Provide a clear description
- Include steps to reproduce
- Share relevant logs or error messages

### Suggesting Features
- Open a discussion first for major features
- Explain the use case and benefits
- Consider implementation complexity

### Submitting Code

1. **Fork the Repository**
   ```bash
   git clone https://github.com/danielberhane/nexusforest-mcp.git
   cd nexusforest-mcp
   ```

2. **Set Up Development Environment**
   ```bash
   # Copy environment template
   cp .env.example .env
   # Edit .env with your credentials

   # Using Docker
   docker-compose up -d

   # Or using Python directly
   pip install -r requirements.txt
   ```

3. **Create a Feature Branch**
   ```bash
   git checkout -b feature/your-feature-name
   ```

4. **Make Your Changes**
   - Follow existing code style
   - Add tests for new functionality
   - Update documentation
   - Ensure all tests pass

5. **Commit Your Changes**
   ```bash
   git commit -m "feat: add new feature"
   ```

   Follow conventional commits:
   - `feat:` New feature
   - `fix:` Bug fix
   - `docs:` Documentation
   - `style:` Code style
   - `refactor:` Code refactoring
   - `test:` Testing
   - `chore:` Maintenance

6. **Push and Create Pull Request**
   ```bash
   git push origin feature/your-feature-name
   ```

## Development Guidelines

### Code Style
- Python: Follow PEP 8
- Use type hints where appropriate
- Maximum line length: 120 characters
- Use descriptive variable names

### Testing
- Write unit tests for new functions
- Ensure existing tests pass
- Aim for >80% code coverage

### Documentation
- Update README for new features
- Add docstrings to functions
- Include usage examples

### Database Changes
- Never modify schema without migration plan
- Test with sample data
- Document any new tables or columns

## Architecture Principles

1. **Separation of Concerns**: Keep MCP protocol, database, and ETL layers independent
2. **Security First**: Always use parameterized queries and validate inputs
3. **Performance**: Consider query optimization and indexing
4. **Maintainability**: Write clean, self-documenting code

## Pull Request Process

1. Ensure all tests pass
2. Update documentation
3. Add entry to CHANGELOG (if exists)
4. Request review from maintainers
5. Address review feedback
6. Merge after approval

## Questions?

For questions about contributing, please:
- Open a GitHub Discussion
- Contact the maintainer through GitHub

## Recognition

Contributors will be recognized in:
- GitHub contributors graph
- README acknowledgments (for significant contributions)
- Release notes

## License

By contributing, you agree that your contributions will be licensed under the Apache 2.0 License.