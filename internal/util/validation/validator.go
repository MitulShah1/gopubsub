package validation

import (
	"fmt"
	"reflect"
	"strings"
)

// Validator provides methods for validating configuration
type Validator struct {
	errors []string
}

// NewValidator creates a new validator
func NewValidator() *Validator {
	return &Validator{
		errors: make([]string, 0),
	}
}

// Validate checks if all validation rules pass
func (v *Validator) Validate() error {
	if len(v.errors) == 0 {
		return nil
	}
	return fmt.Errorf("validation errors: %s", strings.Join(v.errors, "; "))
}

// Required checks if a field is present and not empty
func (v *Validator) Required(fieldName string, value interface{}) *Validator {
	if value == nil {
		v.errors = append(v.errors, fmt.Sprintf("%s is required", fieldName))
		return v
	}

	// Check for empty strings
	if str, ok := value.(string); ok && str == "" {
		v.errors = append(v.errors, fmt.Sprintf("%s cannot be empty", fieldName))
		return v
	}

	// Check for empty slices or maps
	val := reflect.ValueOf(value)
	if val.Kind() == reflect.Slice || val.Kind() == reflect.Map {
		if val.Len() == 0 {
			v.errors = append(v.errors, fmt.Sprintf("%s cannot be empty", fieldName))
		}
	}

	return v
}

// Min checks if a numeric value is greater than or equal to min
func (v *Validator) Min(fieldName string, value, min int) *Validator {
	if value < min {
		v.errors = append(v.errors, fmt.Sprintf("%s must be greater than or equal to %d", fieldName, min))
	}
	return v
}

// Max checks if a numeric value is less than or equal to max
func (v *Validator) Max(fieldName string, value, max int) *Validator {
	if value > max {
		v.errors = append(v.errors, fmt.Sprintf("%s must be less than or equal to %d", fieldName, max))
	}
	return v
}

// Duration checks if a duration is valid
func (v *Validator) Duration(fieldName string, value interface{}) *Validator {
	if value == nil {
		v.errors = append(v.errors, fmt.Sprintf("%s is required", fieldName))
		return v
	}

	switch val := value.(type) {
	case int:
		if val <= 0 {
			v.errors = append(v.errors, fmt.Sprintf("%s must be positive", fieldName))
		}
	case int64:
		if val <= 0 {
			v.errors = append(v.errors, fmt.Sprintf("%s must be positive", fieldName))
		}
	case float64:
		if val <= 0 {
			v.errors = append(v.errors, fmt.Sprintf("%s must be positive", fieldName))
		}
	default:
		v.errors = append(v.errors, fmt.Sprintf("%s must be a numeric value", fieldName))
	}

	return v
}

// URL checks if a string is a valid URL
func (v *Validator) URL(fieldName string, value string) *Validator {
	if value == "" {
		v.errors = append(v.errors, fmt.Sprintf("%s cannot be empty", fieldName))
		return v
	}

	if !strings.HasPrefix(value, "http://") && !strings.HasPrefix(value, "https://") &&
		!strings.HasPrefix(value, "amqp://") && !strings.HasPrefix(value, "amqps://") {
		v.errors = append(v.errors, fmt.Sprintf("%s must be a valid URL", fieldName))
	}

	return v
}

// OneOf checks if a value is one of the allowed values
func (v *Validator) OneOf(fieldName string, value interface{}, allowedValues ...interface{}) *Validator {
	for _, allowed := range allowedValues {
		if value == allowed {
			return v
		}
	}

	v.errors = append(v.errors, fmt.Sprintf("%s must be one of %v", fieldName, allowedValues))
	return v
}

// Custom adds a custom validation error
func (v *Validator) Custom(condition bool, message string) *Validator {
	if !condition {
		v.errors = append(v.errors, message)
	}
	return v
}
