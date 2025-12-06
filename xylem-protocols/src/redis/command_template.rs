//! Command template engine for custom Redis commands
//!
//! Supports variable substitution in command templates:
//! - `__key__` - Replaced with the current key
//! - `__data__` - Replaced with generated value data
//! - `__value_size__` - Replaced with the value size

use anyhow::{anyhow, Result};

/// A parsed command template that can generate RESP requests
#[derive(Debug, Clone, PartialEq)]
pub struct CommandTemplate {
    /// Original template string
    template: String,
    /// Parsed template parts
    parts: Vec<TemplatePart>,
}

#[derive(Debug, Clone, PartialEq)]
enum TemplatePart {
    /// Literal text to include as-is
    Literal(String),
    /// Variable to substitute
    Variable(Variable),
}

#[derive(Debug, Clone, PartialEq)]
enum Variable {
    Key,
    Data,
    ValueSize,
}

impl CommandTemplate {
    /// Parse a command template string
    ///
    /// # Example
    /// ```
    /// use xylem_protocols::CommandTemplate;
    ///
    /// let template = CommandTemplate::parse("HSET myhash __key__ __data__").unwrap();
    /// ```
    pub fn parse(template: &str) -> Result<Self> {
        let mut parts = Vec::new();
        let mut current_literal = String::new();
        let mut chars = template.chars().peekable();

        while let Some(ch) = chars.next() {
            let is_potential_variable = ch == '_' && chars.peek() == Some(&'_');

            if !is_potential_variable {
                current_literal.push(ch);
                continue;
            }

            // Try to parse variable
            let remaining: String = chars.clone().collect();
            let var = Self::try_parse_variable(&remaining);

            if var.is_none() {
                current_literal.push(ch);
                continue;
            }

            // We have a valid variable - save current literal if non-empty
            Self::save_literal_if_nonempty(&mut parts, &mut current_literal);

            // Add variable
            let var = var.unwrap();
            parts.push(TemplatePart::Variable(var.clone()));

            // Skip the variable in the input
            let var_len = Self::get_variable_length(&var) - 1; // -1 because we already consumed first _
            Self::skip_chars(&mut chars, var_len);
        }

        // Save final literal
        Self::save_literal_if_nonempty(&mut parts, &mut current_literal);

        if parts.is_empty() {
            return Err(anyhow!("Empty command template"));
        }

        Ok(Self { template: template.to_string(), parts })
    }

    fn save_literal_if_nonempty(parts: &mut Vec<TemplatePart>, current_literal: &mut String) {
        if !current_literal.is_empty() {
            parts.push(TemplatePart::Literal(current_literal.clone()));
            current_literal.clear();
        }
    }

    fn get_variable_length(var: &Variable) -> usize {
        match var {
            Variable::Key => "__key__".len(),
            Variable::Data => "__data__".len(),
            Variable::ValueSize => "__value_size__".len(),
        }
    }

    fn skip_chars<I: Iterator>(chars: &mut I, count: usize) {
        for _ in 0..count {
            chars.next();
        }
    }

    fn try_parse_variable(s: &str) -> Option<Variable> {
        if s.starts_with("_key__") {
            Some(Variable::Key)
        } else if s.starts_with("_data__") {
            Some(Variable::Data)
        } else if s.starts_with("_value_size__") {
            Some(Variable::ValueSize)
        } else {
            None
        }
    }

    /// Generate a RESP request from this template
    pub fn generate_request(&self, key: u64, value_size: usize) -> Vec<u8> {
        self.generate_request_with_options(key, value_size, "key:", &"x".repeat(value_size))
    }

    /// Generate a RESP request from this template with custom key prefix and value data
    pub fn generate_request_with_options(
        &self,
        key: u64,
        value_size: usize,
        key_prefix: &str,
        value_data: &str,
    ) -> Vec<u8> {
        // First pass: substitute variables to get command parts
        let mut command_parts = Vec::new();

        for part in &self.parts {
            match part {
                TemplatePart::Literal(lit) => {
                    // Split literal by whitespace to get individual arguments
                    // split_whitespace() already skips empty strings
                    command_parts.extend(lit.split_whitespace().map(String::from));
                }
                TemplatePart::Variable(var) => {
                    let value = match var {
                        Variable::Key => format!("{}{}", key_prefix, key),
                        Variable::Data => value_data.to_string(),
                        Variable::ValueSize => value_size.to_string(),
                    };
                    command_parts.push(value);
                }
            }
        }

        // Second pass: encode as RESP array
        let mut request = format!("*{}\r\n", command_parts.len());
        for part in command_parts {
            request.push_str(&format!("${}\r\n{}\r\n", part.len(), part));
        }

        request.into_bytes()
    }

    /// Get the original template string
    pub fn template(&self) -> &str {
        &self.template
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_template() {
        let template = CommandTemplate::parse("GET __key__").unwrap();
        assert_eq!(template.template(), "GET __key__");
    }

    #[test]
    fn test_parse_hset_template() {
        let template = CommandTemplate::parse("HSET myhash __key__ __data__").unwrap();
        assert_eq!(template.template(), "HSET myhash __key__ __data__");
    }

    #[test]
    fn test_parse_all_variables() {
        let template = CommandTemplate::parse("TEST __key__ __data__ __value_size__").unwrap();
        assert_eq!(template.template(), "TEST __key__ __data__ __value_size__");
    }

    #[test]
    fn test_generate_get_request() {
        let template = CommandTemplate::parse("GET __key__").unwrap();
        let request = template.generate_request(42, 64);
        let request_str = String::from_utf8(request).unwrap();

        // Should be: *2\r\n$3\r\nGET\r\n$6\r\nkey:42\r\n
        assert!(request_str.starts_with("*2\r\n"));
        assert!(request_str.contains("GET"));
        assert!(request_str.contains("key:42"));
    }

    #[test]
    fn test_generate_hset_request() {
        let template = CommandTemplate::parse("HSET myhash __key__ __data__").unwrap();
        let request = template.generate_request(100, 5);
        let request_str = String::from_utf8(request).unwrap();

        // Should be: *4\r\n$4\r\nHSET\r\n$7\r\nmyhash\r\n$7\r\nkey:100\r\n$5\r\nxxxxx\r\n
        assert!(request_str.starts_with("*4\r\n"));
        assert!(request_str.contains("HSET"));
        assert!(request_str.contains("myhash"));
        assert!(request_str.contains("key:100"));
        assert!(request_str.contains("xxxxx")); // 5 x's
    }

    #[test]
    fn test_generate_with_value_size() {
        let template = CommandTemplate::parse("SETEX __key__ __value_size__ __data__").unwrap();
        let request = template.generate_request(1, 10);
        let request_str = String::from_utf8(request).unwrap();

        assert!(request_str.contains("SETEX"));
        assert!(request_str.contains("key:1"));
        assert!(request_str.contains("10")); // value_size
        assert!(request_str.contains("xxxxxxxxxx")); // 10 x's
    }

    #[test]
    fn test_empty_template() {
        let result = CommandTemplate::parse("");
        assert!(result.is_err());
    }

    #[test]
    fn test_no_variables() {
        let template = CommandTemplate::parse("PING").unwrap();
        let request = template.generate_request(0, 0);
        let request_str = String::from_utf8(request).unwrap();

        assert!(request_str.starts_with("*1\r\n"));
        assert!(request_str.contains("PING"));
    }

    #[test]
    fn test_complex_template() {
        let template = CommandTemplate::parse("ZADD myset __value_size__ __key__").unwrap();
        let request = template.generate_request(50, 100);
        let request_str = String::from_utf8(request).unwrap();

        // *4\r\n$4\r\nZADD\r\n$5\r\nmyset\r\n$3\r\n100\r\n$6\r\nkey:50\r\n
        assert!(request_str.starts_with("*4\r\n"));
        assert!(request_str.contains("ZADD"));
        assert!(request_str.contains("myset"));
        assert!(request_str.contains("100"));
        assert!(request_str.contains("key:50"));
    }

    #[test]
    fn test_template_with_underscore_literal() {
        // Test template with underscore that doesn't match any variable
        let template = CommandTemplate::parse("SET _custom_key __key__").unwrap();
        let request = template.generate_request(42, 100);
        let request_str = String::from_utf8_lossy(&request);

        // _custom_key should be treated as a literal (line 73 coverage)
        assert!(request_str.contains("_custom_key"));
        assert!(request_str.contains("key:42"));
    }

    #[test]
    fn test_template_with_invalid_variable() {
        // Test that templates with underscores that don't match variables
        // are treated as literals (line 100 coverage)
        let template = CommandTemplate::parse("SET _invalid__ __key__").unwrap();
        let request = template.generate_request(42, 100);
        let request_str = String::from_utf8_lossy(&request);

        // _invalid__ should be treated as a literal
        assert!(request_str.contains("_invalid__"));
        assert!(request_str.contains("key:42"));
    }
}
